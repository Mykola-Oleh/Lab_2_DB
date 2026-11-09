using System.Diagnostics;
using System.Globalization;
using System.Text;
using Dapper;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using Npgsql;
using StackExchange.Redis;

Console.OutputEncoding = Encoding.UTF8;
var count = ReadArg("--students", 1000);
var rounds = ReadArg("--rounds", 300);
var perStudent = ReadArg("--enrollments", 8);
if (count < 10 || rounds < 10 || perStudent < 1) throw new ArgumentException("Minimum: 10 students, 10 rounds, 1 enrollment");
var pgConnection = Environment.GetEnvironmentVariable("UNI_DB")
    ?? "Host=localhost;Port=5432;Database=university;Username=postgres;Password=postgres";

var mongoConnection = Environment.GetEnvironmentVariable("MONGO_URL")
    ?? "mongodb://localhost:27018";
var redisConnection = Environment.GetEnvironmentVariable("REDIS_URL")
    ?? "localhost:6380";

await using var pg = new NpgsqlConnection(pgConnection);
await pg.OpenAsync();
var mongo = new MongoClient(mongoConnection).GetDatabase("university_nosql_lab");
var profiles = mongo.GetCollection<StudentProfile>("student_profiles");
await using var redis = await ConnectionMultiplexer.ConnectAsync(redisConnection);

Console.WriteLine($"Students={count:N0}, enrollments/student={perStudent}, rounds={rounds}");
await pg.ExecuteAsync("""
CREATE SCHEMA IF NOT EXISTS nosql_lab;
CREATE TABLE IF NOT EXISTS nosql_lab.groups (group_id integer PRIMARY KEY, name text NOT NULL);
CREATE TABLE IF NOT EXISTS nosql_lab.students (student_id integer PRIMARY KEY, group_id integer NOT NULL REFERENCES nosql_lab.groups(group_id), full_name text NOT NULL);
CREATE TABLE IF NOT EXISTS nosql_lab.courses (course_id integer PRIMARY KEY, code text NOT NULL, title text NOT NULL);
CREATE TABLE IF NOT EXISTS nosql_lab.enrollments (student_id integer NOT NULL REFERENCES nosql_lab.students(student_id), course_id integer NOT NULL REFERENCES nosql_lab.courses(course_id), semester_id integer NOT NULL, status text NOT NULL, PRIMARY KEY(student_id,course_id));
CREATE INDEX IF NOT EXISTS ix_lab_students_group ON nosql_lab.students(group_id);
CREATE INDEX IF NOT EXISTS ix_lab_enrollments_student ON nosql_lab.enrollments(student_id);
""");
await profiles.Indexes.CreateOneAsync(new CreateIndexModel<StudentProfile>(Builders<StudentProfile>.IndexKeys.Ascending(x => x.GroupId)));

var courseCount = Math.Max(perStudent * 4, 40);
var courseRows = Enumerable.Range(1, courseCount).Select(i => new CourseRow(i, $"C{i:0000}", $"Course {i}")).ToArray();
var data = Enumerable.Range(1, count).Select(i => new StudentProfile
{
    StudentId = i,
    GroupId = (i - 1) % 100 + 1,
    FullName = $"Student {i:000000}",
    Enrollments = Enumerable.Range(0, perStudent).Select(j =>
    {
        var c = courseRows[((i - 1) * 7 + j) % courseCount];
        return new EnrollmentDoc { CourseId = c.Id, CourseCode = c.Code, CourseTitle = c.Title, SemesterId = j % 2 + 1, Status = "active" };
    }).ToList()
}).ToArray();

Console.WriteLine("Seeding PostgreSQL and MongoDB (this replaces only lab data)...");
await pg.ExecuteAsync("TRUNCATE nosql_lab.enrollments, nosql_lab.students, nosql_lab.courses, nosql_lab.groups CASCADE");
await profiles.DeleteManyAsync(FilterDefinition<StudentProfile>.Empty);
await pg.ExecuteAsync("INSERT INTO nosql_lab.groups(group_id,name) SELECT i, 'Group '||i FROM generate_series(1,100) i");
await pg.ExecuteAsync("INSERT INTO nosql_lab.courses(course_id,code,title) VALUES (@Id,@Code,@Title)", courseRows);
using (var writer = pg.BeginBinaryImport("COPY nosql_lab.students(student_id,group_id,full_name) FROM STDIN (FORMAT BINARY)"))
{
    foreach (var s in data) { writer.StartRow(); writer.Write(s.StudentId); writer.Write(s.GroupId); writer.Write(s.FullName); }
    writer.Complete();
}
using (var writer = pg.BeginBinaryImport("COPY nosql_lab.enrollments(student_id,course_id,semester_id,status) FROM STDIN (FORMAT BINARY)"))
{
    foreach (var s in data) foreach (var e in s.Enrollments)
    { writer.StartRow(); writer.Write(s.StudentId); writer.Write(e.CourseId); writer.Write(e.SemesterId); writer.Write(e.Status); }
    writer.Complete();
}
foreach (var chunk in data.Chunk(1000)) await profiles.InsertManyAsync(chunk);
await pg.ExecuteAsync("ANALYZE nosql_lab.students; ANALYZE nosql_lab.enrollments; ANALYZE nosql_lab.courses;");

const string byStudentSql = """
SELECT s.student_id AS StudentId, s.group_id AS GroupId, s.full_name AS FullName,
       e.course_id AS CourseId, c.code AS CourseCode, c.title AS CourseTitle,
       e.semester_id AS SemesterId, e.status AS Status
FROM nosql_lab.students s
JOIN nosql_lab.enrollments e ON e.student_id=s.student_id
JOIN nosql_lab.courses c ON c.course_id=e.course_id
WHERE s.student_id=@id ORDER BY e.course_id
""";
const string byGroupSql = """
SELECT s.student_id AS StudentId, s.group_id AS GroupId, s.full_name AS FullName,
       e.course_id AS CourseId, c.code AS CourseCode, c.title AS CourseTitle,
       e.semester_id AS SemesterId, e.status AS Status
FROM nosql_lab.students s
JOIN nosql_lab.enrollments e ON e.student_id=s.student_id
JOIN nosql_lab.courses c ON c.course_id=e.course_id
WHERE s.group_id=@id ORDER BY s.student_id,e.course_id
""";

foreach (var id in new[] { 1, count / 2, count })
{
    var sql = (await pg.QueryAsync<FlatRow>(byStudentSql, new { id })).ToArray();
    var doc = await profiles.Find(x => x.StudentId == id).FirstOrDefaultAsync() ?? throw new Exception("Missing Mongo profile");
    if (sql.Length != doc.Enrollments.Count || sql.Any(r => r.GroupId != doc.GroupId || r.FullName != doc.FullName ||
        !doc.Enrollments.Any(e => e.CourseId == r.CourseId && e.CourseCode == r.CourseCode && e.CourseTitle == r.CourseTitle && e.SemesterId == r.SemesterId && e.Status == r.Status)))
        throw new Exception($"SQL/Mongo mismatch for student {id}");
}
foreach (var id in new[] { 1, 50, 100 })
{
    var sql = (await pg.QueryAsync<FlatRow>(byGroupSql, new { id })).ToArray();
    var docs = await profiles.Find(x => x.GroupId == id).ToListAsync();
    if (sql.Length != docs.Sum(x => x.Enrollments.Count)) throw new Exception("Group result mismatch");
}
Console.WriteLine("Validation OK.");

var results = new List<Metric>();
var rng = new Random(2026);
var studentIds = Enumerable.Range(0, rounds).Select(_ => rng.Next(1, count + 1)).ToArray();
var groupIds = Enumerable.Range(0, rounds).Select(_ => rng.Next(1, 101)).ToArray();

async Task Measure(string operation, string db, Func<int, Task<int>> action, int[] ids)
{
    foreach (var id in ids.Take(Math.Min(30, ids.Length))) await action(id);
    var samples = new double[ids.Length];
    long rows = 0;
    for (var i = 0; i < ids.Length; i++)
    {
        var watch = Stopwatch.StartNew();
        rows += await action(ids[i]);
        watch.Stop();
        samples[i] = watch.Elapsed.TotalMilliseconds;
    }
    Array.Sort(samples);
    var metric = new Metric(count, perStudent, operation, db, ids.Length, samples.Average(), samples[samples.Length / 2], samples[(int)Math.Ceiling(.95 * samples.Length) - 1], rows);
    results.Add(metric);
    Console.WriteLine($"{operation,-14} {db,-11} avg={metric.MeanMs,9:F3} ms  p50={metric.P50Ms,9:F3}  p95={metric.P95Ms,9:F3}");
}

await Measure("student_read", "PostgreSQL", async id => (await pg.QueryAsync<FlatRow>(byStudentSql, new { id })).Count(), studentIds);
await Measure("student_read", "MongoDB", async id => (await profiles.Find(x => x.StudentId == id).FirstOrDefaultAsync())?.Enrollments.Count ?? 0, studentIds);
await Measure("group_read", "PostgreSQL", async id => (await pg.QueryAsync<FlatRow>(byGroupSql, new { id })).Count(), groupIds);
await Measure("group_read", "MongoDB", async id => (await profiles.Find(x => x.GroupId == id).ToListAsync()).Sum(x => x.Enrollments.Count), groupIds);

var updateIds = studentIds.Distinct().ToArray();
await Measure("status_update", "PostgreSQL", async id => await pg.ExecuteAsync(
    "UPDATE nosql_lab.enrollments SET status=CASE WHEN status='active' THEN 'completed' ELSE 'active' END WHERE student_id=@id AND course_id=(SELECT min(course_id) FROM nosql_lab.enrollments WHERE student_id=@id)", new { id }), updateIds);
await Measure("status_update", "MongoDB", async id =>
{
    var doc = await profiles.Find(x => x.StudentId == id).FirstAsync();
    var target = doc.Enrollments.MinBy(e => e.CourseId)!;
    var status = target.Status == "active" ? "completed" : "active";
    var result = await profiles.UpdateOneAsync(x => x.StudentId == id && x.Enrollments.Any(e => e.CourseId == target.CourseId),
        Builders<StudentProfile>.Update.Set("enrollments.$.status", status));
    return (int)result.ModifiedCount;
}, updateIds);

var cache = redis.GetDatabase();
await cache.StringSetAsync("university:lab:student:1", "Student 000001", TimeSpan.FromMinutes(5));
Console.WriteLine($"Redis GET: {await cache.StringGetAsync("university:lab:student:1")}");
Console.WriteLine($"Redis EXISTS: {await cache.KeyExistsAsync("university:lab:student:1")}");
Console.WriteLine($"Redis TTL: {await cache.KeyTimeToLiveAsync("university:lab:student:1")}");
Console.WriteLine($"Redis INCR: {await cache.StringIncrementAsync("university:lab:counter")}");
await cache.KeyDeleteAsync("university:lab:student:1");
Console.WriteLine($"Redis DELETE -> exists: {await cache.KeyExistsAsync("university:lab:student:1")}");

var path = Path.GetFullPath($"results_{count}_{DateTime.UtcNow:yyyyMMdd_HHmmss}.csv");
var csv = new StringBuilder("students,enrollments_per_student,operation,database,rounds,mean_ms,p50_ms,p95_ms,returned_rows\n");
foreach (var r in results) csv.AppendLine(string.Join(',', r.Students, r.EnrollmentsPerStudent, r.Operation, r.Database, r.Rounds,
    r.MeanMs.ToString("F6", CultureInfo.InvariantCulture), r.P50Ms.ToString("F6", CultureInfo.InvariantCulture),
    r.P95Ms.ToString("F6", CultureInfo.InvariantCulture), r.ReturnedRows));
await File.WriteAllTextAsync(path, csv.ToString(), Encoding.UTF8);
Console.WriteLine($"Saved: {path}");

int ReadArg(string name, int fallback)
{
    var index = Array.IndexOf(args, name);
    return index >= 0 && index + 1 < args.Length ? int.Parse(args[index + 1], CultureInfo.InvariantCulture) : fallback;
}

public sealed class StudentProfile
{
    [BsonId] public int StudentId { get; set; }
    [BsonElement("groupId")] public int GroupId { get; set; }
    [BsonElement("fullName")] public string FullName { get; set; } = "";
    [BsonElement("enrollments")] public List<EnrollmentDoc> Enrollments { get; set; } = new();
}
public sealed class EnrollmentDoc
{
    [BsonElement("courseId")] public int CourseId { get; set; }
    [BsonElement("courseCode")] public string CourseCode { get; set; } = "";
    [BsonElement("courseTitle")] public string CourseTitle { get; set; } = "";
    [BsonElement("semesterId")] public int SemesterId { get; set; }
    [BsonElement("status")] public string Status { get; set; } = "";
}
public sealed record CourseRow(int Id, string Code, string Title);
public sealed class FlatRow
{
    public int StudentId { get; set; }
    public int GroupId { get; set; }
    public string FullName { get; set; } = "";
    public int CourseId { get; set; }
    public string CourseCode { get; set; } = "";
    public string CourseTitle { get; set; } = "";
    public int SemesterId { get; set; }
    public string Status { get; set; } = "";
}
public sealed record Metric(int Students, int EnrollmentsPerStudent, string Operation, string Database, int Rounds, double MeanMs, double P50Ms, double P95Ms, long ReturnedRows);
