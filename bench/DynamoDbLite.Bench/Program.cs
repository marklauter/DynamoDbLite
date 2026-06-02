using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite;
using Microsoft.Data.Sqlite;
using System.Diagnostics;

const int N = 10000;
const int MaxBatch = 500;                        // raise the BatchWriteItem cap so 500-item batches are accepted
const int Warmup = 200;
const string Hk = "01HXY3ZJ8K9QWE7TUV2RNM4B6C";  // single partition key, ULID-style 26-char string; every sk is unique

var ac0 = new KeyValuePair<string, string>("wal_autocheckpoint", "0");
var ac100 = new KeyValuePair<string, string>("wal_autocheckpoint", "100");
var ac500 = new KeyValuePair<string, string>("wal_autocheckpoint", "500");

// pre-generate everything outside the timed loops
var warmReqs = new PutItemRequest[Warmup];
for (var w = 0; w < Warmup; w++)
    warmReqs[w] = new PutItemRequest { TableName = "T", Item = Item(MakeSk("wm", w)) };

var singleReqs = new PutItemRequest[N];
for (var i = 0; i < N; i++)
    singleReqs[i] = new PutItemRequest { TableName = "T", Item = Item(MakeSk("sk", i)) };

var batch25 = BuildBatches(25);
var batch500 = BuildBatches(500);

List<BatchWriteItemRequest> BuildBatches(int size)
{
    var list = new List<BatchWriteItemRequest>();
    for (var i = 0; i < N;)
    {
        var writes = new List<WriteRequest>(size);
        for (var j = 0; j < size && i < N; j++, i++)
            writes.Add(new WriteRequest { PutRequest = new PutRequest { Item = singleReqs[i].Item } });
        list.Add(new BatchWriteItemRequest { RequestItems = new Dictionary<string, List<WriteRequest>> { ["T"] = writes } });
    }

    return list;
}

(string Label, bool File, bool Wal, KeyValuePair<string, string>[] Pragmas, int Repeats)[] configs =
[
    ("inmem",                       false, false, [],       5),
    ("file: default-journal",       true,  false, [],       1),
    ("file: WAL ac=1000 (default)", true,  true,  [],       5),
    ("file: WAL ac=500",            true,  true,  [ac500],  5),
    ("file: WAL ac=100",            true,  true,  [ac100],  5),
    ("file: WAL ac=0 (none)",       true,  true,  [ac0],    5),
];

Console.WriteLine($"N={N}, maxBatch={MaxBatch}, warmup={Warmup}, hk={Hk.Length}ch, sk={singleReqs[0].Item["sk"].S.Length}ch, text={Payload.Text.Length}B  (med ms; b500 shows med/min)");

foreach (var (label, file, wal, pragmas, repeats) in configs)
{
    var (single, _) = await MedianAsync(file, wal, pragmas, repeats, SingleRun);
    var (b25, _) = await MedianAsync(file, wal, pragmas, repeats, o => BatchRun(o, batch25));
    var (b500, b500Min) = await MedianAsync(file, wal, pragmas, repeats, o => BatchRun(o, batch500));
    Console.WriteLine(
        $"{label,-31} single={single,7:F0}  batch25={b25,6:F0}  batch500={b500,6:F0}/{b500Min,-6:F0}");
}

try
{
    Directory.Delete(Path.Combine(Path.GetTempPath(), "ddblite-bench-dbs"), recursive: true);
}
catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or DirectoryNotFoundException)
{
    // best-effort cleanup
}

// ── Timed runners (non-static so they use the pre-built request arrays) ──

async Task<double> SingleRun(DynamoDbLiteOptions options)
{
    using var client = new DynamoDbClient(options);
    await CreateTableAsync(client);
    await WarmAsync(client);
    SettleGc();

    var sw = Stopwatch.StartNew();
    foreach (var req in singleReqs)
        _ = await client.PutItemAsync(req);
    sw.Stop();
    return sw.Elapsed.TotalMilliseconds;
}

async Task<double> BatchRun(DynamoDbLiteOptions options, List<BatchWriteItemRequest> batches)
{
    using var client = new DynamoDbClient(options);
    await CreateTableAsync(client);
    await WarmAsync(client);
    SettleGc();

    var sw = Stopwatch.StartNew();
    foreach (var req in batches)
        _ = await client.BatchWriteItemAsync(req);
    sw.Stop();
    return sw.Elapsed.TotalMilliseconds;
}

async Task WarmAsync(DynamoDbClient client)
{
    foreach (var req in warmReqs)
        _ = await client.PutItemAsync(req);
}

// ── Static helpers ──

static async Task<(double Med, double Min)> MedianAsync(bool file, bool wal, KeyValuePair<string, string>[] pragmas,
    int repeats, Func<DynamoDbLiteOptions, Task<double>> run)
{
    var samples = new double[repeats];
    for (var r = 0; r < repeats; r++)
        samples[r] = await MeasureAsync(file, wal, pragmas, run);

    Array.Sort(samples);
    var med = samples.Length % 2 == 1
        ? samples[samples.Length / 2]
        : (samples[samples.Length / 2 - 1] + samples[samples.Length / 2]) / 2;
    return (med, samples[0]);
}

static async Task<double> MeasureAsync(bool file, bool wal, KeyValuePair<string, string>[] pragmas,
    Func<DynamoDbLiteOptions, Task<double>> run)
{
    var options = file ? FileOptions(wal, pragmas, out var dbPath) : MemOptions(out dbPath);
    var ms = await run(options);

    if (dbPath is not null)
    {
        SqliteConnection.ClearAllPools();
        foreach (var f in new[] { dbPath, dbPath + "-wal", dbPath + "-shm", dbPath + "-journal" })
        {
            try
            {
                if (File.Exists(f))
                    File.Delete(f);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                // temp file, best-effort cleanup
            }
        }
    }

    return ms;
}

static DynamoDbLiteOptions MemOptions(out string? path)
{
    path = null;
    return new DynamoDbLiteOptions($"Data Source=mem_{Guid.NewGuid():N};Mode=Memory;Cache=Shared")
    {
        MaxBatchWriteItems = MaxBatch,
    };
}

static string BenchDir() => Path.Combine(Path.GetTempPath(), "ddblite-bench-dbs");

static DynamoDbLiteOptions FileOptions(bool wal, KeyValuePair<string, string>[] pragmas, out string? path)
{
    _ = Directory.CreateDirectory(BenchDir());
    path = Path.Combine(BenchDir(), $"bench_{Guid.NewGuid():N}.db");
    return new DynamoDbLiteOptions($"Data Source={path}", UseWriteAheadLog: wal)
    {
        Pragmas = pragmas,
        MaxBatchWriteItems = MaxBatch,
    };
}

static Task CreateTableAsync(DynamoDbClient client) =>
    client.CreateTableAsync(new CreateTableRequest
    {
        TableName = "T",
        KeySchema =
        [
            new KeySchemaElement { AttributeName = "hk", KeyType = KeyType.HASH },
            new KeySchemaElement { AttributeName = "sk", KeyType = KeyType.RANGE },
        ],
        AttributeDefinitions =
        [
            new AttributeDefinition { AttributeName = "hk", AttributeType = ScalarAttributeType.S },
            new AttributeDefinition { AttributeName = "sk", AttributeType = ScalarAttributeType.S },
        ],
        ProvisionedThroughput = new ProvisionedThroughput { ReadCapacityUnits = 5, WriteCapacityUnits = 5 },
    });

static string MakeSk(string prefix, int i) => $"{prefix}-{i:D10}-".PadRight(60, 'x');

static Dictionary<string, AttributeValue> Item(string sk) => new()
{
    ["hk"] = new AttributeValue { S = Hk },
    ["sk"] = new AttributeValue { S = sk },
    ["text"] = new AttributeValue { S = Payload.Text },
};

static void SettleGc()
{
    GC.Collect();
    GC.WaitForPendingFinalizers();
    GC.Collect();
}

internal static class Payload
{
    public static readonly string Text = new('x', 2000);
}
