// Target: net8.0
// NuGet: Npgsql, Microsoft.Data.SqlClient

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using NpgsqlTypes;

#pragma warning disable CS1998

// ======================================================
// Config
// ======================================================
sealed class Config
{
    // IO
    public string OwnershipCSV = Env("OWNERSHIP_CSV", "RSV_vlastnik_provozovatel_vozidla_20250901.csv");
    public string VehicleCSV   = Env("VEHICLE_CSV",   "RSV_vypis_vozidel_20250902.csv");
    public string SkippedDir   = Env("SKIPPED_DIR",   "./skipped");

    // DB
    public string DBDriver     = Env("DB_DRIVER", "postgres"); // postgres|mssql
    public string DSN          = Environment.GetEnvironmentVariable("DB_DSN") ?? "";
    public string DBUser       = Env("DB_USER", "user");
    public string DBPassword   = Env("DB_PASSWORD", "password");
    public string DBHost       = Env("DB_HOST", "localhost");
    public string DBPort       = Env("DB_PORT", "5432");
    public string DBName       = Env("DB_NAME", "testdb");

    // Import
    public int OwnershipWorkers = Int("OWNERSHIP_WORKERS", 8);
    public int TechWorkers      = Int("TECH_WORKERS", 4);
    public int BatchSize        = Int("BATCH_SIZE", 1_000);
    public int FlushMs          = Int("FLUSH_MS", 250); // time-based flush in ms

    // Misc
    public bool UnloggedTables  = Bool("PG_UNLOGGED", true);

    public void ApplyArgs(string[] args)
    {
        for (int i = 0; i < args.Length; i++)
        {
            string key, value;
            if (args[i].StartsWith("--") && args[i].Contains('='))
            {
                var parts = args[i].Split('=', 2);
                key = parts[0]; value = parts[1];
            }
            else if (args[i].StartsWith("--") && i + 1 < args.Length && !args[i + 1].StartsWith("--"))
            {
                key = args[i]; value = args[++i];
            }
            else continue;

            switch (key)
            {
                case "--ownership_csv": OwnershipCSV = value; break;
                case "--vehicle_csv":   VehicleCSV   = value; break;
                case "--skipped_dir":   SkippedDir   = value; break;
                case "--db_driver":     DBDriver     = value; break;
                case "--dsn":           DSN          = value; break;
                case "--db_user":       DBUser       = value; break;
                case "--db_password":   DBPassword   = value; break;
                case "--db_host":       DBHost       = value; break;
                case "--db_port":       DBPort       = value; break;
                case "--db_name":       DBName       = value; break;
                case "--ownership_workers": OwnershipWorkers = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "--tech_workers":      TechWorkers      = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "--batch_size":        BatchSize        = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "--flush_ms":          FlushMs          = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "--pg_unlogged":       UnloggedTables   = ParseBool(value); break;
            }
        }

        static bool ParseBool(string s) =>
            s.Equals("1", StringComparison.OrdinalIgnoreCase) ||
            s.Equals("true", StringComparison.OrdinalIgnoreCase) ||
            s.Equals("yes", StringComparison.OrdinalIgnoreCase) ||
            s.Equals("on", StringComparison.OrdinalIgnoreCase);
    }

    public string BuildPgDsn()
        => $"Host={DBHost};Port={DBPort};Username={DBUser};Password={DBPassword};Database={DBName}";

    static string Env(string k, string d) => Environment.GetEnvironmentVariable(k) is { Length: >0 } v ? v : d;
    static int Int(string k, int d) => int.TryParse(Environment.GetEnvironmentVariable(k), NumberStyles.Integer, CultureInfo.InvariantCulture, out var i) ? i : d;
    static bool Bool(string k, bool d)
    {
        var v = Environment.GetEnvironmentVariable(k);
        if (string.IsNullOrEmpty(v)) return d;
        return v.Equals("1", StringComparison.OrdinalIgnoreCase) ||
               v.Equals("true", StringComparison.OrdinalIgnoreCase) ||
               v.Equals("yes", StringComparison.OrdinalIgnoreCase) ||
               v.Equals("on", StringComparison.OrdinalIgnoreCase);
    }
}

// ======================================================
// Logging (no external packages) - structured, lightweight
// ======================================================
enum LogLevel { Trace, Debug, Information, Warning, Error, Critical }

sealed class SimpleLogger
{
    private readonly string _name;
    private static readonly object _lock = new();

    public SimpleLogger(string name) => _name = name;

    private static string Now() => DateTime.UtcNow.ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture);

    private static ConsoleColor ColorFor(LogLevel lvl) => lvl switch
    {
        LogLevel.Trace => ConsoleColor.DarkGray,
        LogLevel.Debug => ConsoleColor.Gray,
        LogLevel.Information => ConsoleColor.White,
        LogLevel.Warning => ConsoleColor.Yellow,
        LogLevel.Error => ConsoleColor.Red,
        LogLevel.Critical => ConsoleColor.Red,
        _ => ConsoleColor.White
    };

    private static void Write(LogLevel lvl, string msg)
    {
        lock (_lock)
        {
            var prev = Console.ForegroundColor;
            try { Console.ForegroundColor = ColorFor(lvl); Console.WriteLine(msg); }
            finally { Console.ForegroundColor = prev; }
        }
    }

    // Supports numeric {0} style and named {Name} style. Never throws.
    private static string TryFormat(string template, object?[] args)
    {
        if (args is not { Length: > 0 }) return template;

        // If template already looks purely numeric, try fast path.
        if (template.IndexOf('{') >= 0 && Regex.IsMatch(template, @"\{(\d+)(:[^}]*)?\}"))
        {
            try { return string.Format(CultureInfo.InvariantCulture, template, args); }
            catch { /* fall through to safe path */ }
        }

        // Map unique {Name} tokens to {0},{1}...
        try
        {
            var names = new List<string>();
            var sb = new StringBuilder(template.Length + 32);
            for (int i = 0; i < template.Length; i++)
            {
                char c = template[i];
                if (c == '{')
                {
                    int j = i + 1;
                    if (j < template.Length && template[j] == '{') { sb.Append("{{"); i++; continue; }

                    int start = j;
                    while (j < template.Length && template[j] != '}') j++;
                    if (j >= template.Length) { sb.Append(template.AsSpan(i)); break; }

                    var token = template.Substring(start, j - start);
                    if (int.TryParse(token, NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
                    {
                        sb.Append('{').Append(token).Append('}');
                    }
                    else
                    {
                        int idx = names.IndexOf(token);
                        if (idx < 0) { idx = names.Count; names.Add(token); }
                        sb.Append('{').Append(idx.ToString(CultureInfo.InvariantCulture)).Append('}');
                    }
                    i = j;
                }
                else if (c == '}' && i + 1 < template.Length && template[i + 1] == '}')
                {
                    sb.Append("}}"); i++;
                }
                else
                {
                    sb.Append(c);
                }
            }

            return string.Format(CultureInfo.InvariantCulture, sb.ToString(), args);
        }
        catch
        {
            return template + " | " + string.Join(", ", args.Select(a => a?.ToString() ?? "null"));
        }
    }

    private static string Format(LogLevel lvl, string name, string template, params object?[] args)
        => $"{Now()} [{lvl}] {name} {TryFormat(template, args)}";

    public void Log(LogLevel lvl, string template, params object?[] args)
        => Write(lvl, Format(lvl, _name, template, args));

    public void LogInformation(string template, params object?[] args) => Log(LogLevel.Information, template, args);
    public void LogWarning(string template, params object?[] args)     => Log(LogLevel.Warning, template, args);
    public void LogError(string template, params object?[] args)       => Log(LogLevel.Error, template, args);
    public void LogDebug(string template, params object?[] args)       => Log(LogLevel.Debug, template, args);

    public void LogDebug(Exception ex, string template, params object?[] args)
        => Log(LogLevel.Debug, template + " | " + ex, args);
    public void LogInformation(Exception ex, string template, params object?[] args)
        => Log(LogLevel.Information, template + " | " + ex, args);
    public void LogWarning(Exception ex, string template, params object?[] args)
        => Log(LogLevel.Warning, template + " | " + ex, args);
    public void LogError(Exception ex, string template, params object?[] args)
        => Log(LogLevel.Error, template + " | " + ex, args);
}

static class LogNames
{
    public static SimpleLogger OwnershipPipeline()   => new SimpleLogger("OwnershipPipeline");
    public static SimpleLogger VehicleTechPipeline() => new SimpleLogger("VehicleTechPipeline");
    public static SimpleLogger PgOwnershipWriter()   => new SimpleLogger("PgOwnershipWriter");
    public static SimpleLogger PgVehicleTechWriter() => new SimpleLogger("PgVehicleTechWriter");
    public static SimpleLogger SqlOwnershipWriter()  => new SimpleLogger("SqlOwnershipWriter");
    public static SimpleLogger SqlVehicleTechWriter()=> new SimpleLogger("SqlVehicleTechWriter");
    public static SimpleLogger Program()             => new SimpleLogger("Program");
}

// ======================================================
// CSV helpers (dirty, resilient)
// ======================================================
static class Csv
{
    private static readonly Regex DateRe = new(@"^\d{2}\.\d{2}\.\d{4}$", RegexOptions.Compiled);

    public static async Task<string?> ReadLogicalCsvLineAsync(StreamReader r, CancellationToken ct)
    {
        var sb = new StringBuilder();
        bool inQuotes = false, atStart = true, first = true;

        while (true)
        {
            var raw = await r.ReadLineAsync(ct).ConfigureAwait(false);
            if (raw is null)
            {
                if (sb.Length == 0) return null;
                return sb.ToString();
            }
            var part = raw.AsSpan().TrimEnd(new[] { '\r', '\n' }).ToString();

            if (!first && inQuotes) sb.Append("\r\n");
            sb.Append(part);
            first = false;

            int i = 0;
            while (i < part.Length)
            {
                var ch = part[i];
                switch (ch)
                {
                    case ',':
                        if (!inQuotes) atStart = true;
                        i++; break;
                    case '"':
                        if (inQuotes)
                        {
                            if (i + 1 < part.Length && part[i + 1] == '"') { i += 2; continue; }
                            int j = i + 1;
                            while (j < part.Length && (j < part.Length && (part[j] == ' ' || part[j] == '\t'))) j++;
                            if (j >= part.Length || part[j] == ',') { inQuotes = false; atStart = false; i++; continue; }
                            i++;
                        }
                        else
                        {
                            if (atStart) { inQuotes = true; atStart = false; i++; }
                            else i++;
                        }
                        break;
                    default:
                        if (!inQuotes) atStart = false;
                        i++; break;
                }
            }

            if (!inQuotes) return sb.ToString();
        }
    }

    public static string[] ParseCsvLineLoose(string line)
    {
        var fields = new List<string>();
        var sb = new StringBuilder();
        bool inQuotes = false, atStart = true;
        int i = 0;

        while (i < line.Length)
        {
            char ch = line[i];
            switch (ch)
            {
                case ',':
                    if (inQuotes) sb.Append(',');
                    else { fields.Add(sb.ToString()); sb.Clear(); atStart = true; }
                    i++; break;
                case '"':
                    if (inQuotes)
                    {
                        if (i + 1 < line.Length && line[i + 1] == '"')
                        {
                            int j = i + 2;
                            while (j < line.Length && (line[j] == ' ' || line[j] == '\t')) j++;
                            if (j >= line.Length || line[j] == ',') { sb.Append('"'); inQuotes = false; atStart = false; i += 2; continue; }
                            sb.Append('"'); i += 2; continue;
                        }
                        int k = i + 1;
                        while (k < line.Length && (line[k] == ' ' || line[k] == '\t')) k++;
                        if (k >= line.Length || line[k] == ',') { inQuotes = false; atStart = false; i++; continue; }
                        sb.Append('"'); i++;
                    }
                    else
                    {
                        if (atStart) { inQuotes = true; atStart = false; i++; }
                        else { sb.Append('"'); i++; }
                    }
                    break;
                default:
                    sb.Append(ch);
                    if (!inQuotes) atStart = false;
                    i++; break;
            }
        }
        fields.Add(sb.ToString());
        return fields.ToArray();
    }

    public static (string[] repaired, bool ok) RepairOverlongCommaFields(string[] fields)
    {
        if (fields.Length <= 9) return (Array.Empty<string>(), false);
        int last = fields.Length - 1;
        var d2 = fields[last];
        var d1 = fields[last - 1];
        if (!(d2 == "" || DateRe.IsMatch(d2.Trim()))) return (Array.Empty<string>(), false);
        if (!(d1 == "" || DateRe.IsMatch(d1.Trim()))) return (Array.Empty<string>(), false);
        int addrIdx = last - 2;
        if (addrIdx < 6) return (Array.Empty<string>(), false);

        var head = fields.Take(5).ToArray();
        var name = string.Join(",", fields.Skip(5).Take(addrIdx - 5));
        var addr = fields[addrIdx];

        var outFlds = new List<string>(9);
        outFlds.AddRange(head);
        outFlds.Add(name);
        outFlds.Add(addr);
        outFlds.Add(d1);
        outFlds.Add(d2);
        if (outFlds.Count != 9) return (Array.Empty<string>(), false);
        return (outFlds.ToArray(), true);
    }

    public static string[] LexBySpaceWithQuotes(string line)
    {
        var tokens = new List<string>();
        var sb = new StringBuilder();
        bool inQuotes = false;
        int i = 0;
        void Flush() { if (sb.Length > 0) { tokens.Add(sb.ToString()); sb.Clear(); } }

        while (i < line.Length)
        {
            char ch = line[i];
            switch (ch)
            {
                case '"':
                    if (inQuotes)
                    {
                        if (i + 1 < line.Length && line[i + 1] == '"') { sb.Append('"'); i += 2; continue; }
                        inQuotes = false; i++;
                    }
                    else { inQuotes = true; i++; }
                    break;
                case ' ':
                case '\t':
                    if (inQuotes) { sb.Append(ch); i++; }
                    else { Flush(); while (i < line.Length && (line[i] == ' ' || line[i] == '\t')) i++; }
                    break;
                default:
                    sb.Append(ch); i++; break;
            }
        }
        Flush();
        return tokens.ToArray();
    }

    public static (string[] row, bool ok) ParseSpaceSeparatedRow(string line)
    {
        var toks = LexBySpaceWithQuotes(line).ToList();
        if (toks.Count < 6) return (Array.Empty<string>(), false);
        string d2 = "", d1 = "";
        if (toks.Count >= 1 && Regex.IsMatch(toks[^1], @"^\d{2}\.\d{2}\.\d{4}$")) { d2 = toks[^1]; toks.RemoveAt(toks.Count - 1); }
        if (toks.Count >= 1 && Regex.IsMatch(toks[^1], @"^\d{2}\.\d{2}\.\d{4}$")) { d1 = toks[^1]; toks.RemoveAt(toks.Count - 1); }
        if (toks.Count < 5) return (Array.Empty<string>(), false);

        var pcv = toks[0]; var typ = toks[1]; var vztah = toks[2]; var aktualni = toks[3]; var ico = toks[4];
        var rest = toks.Skip(5).ToList();
        if (rest.Count == 0) return (Array.Empty<string>(), false);
        var name = rest[0];
        rest = rest.Skip(1).ToList();
        var addr = string.Join(" ", rest);
        var outRow = new List<string> { pcv, typ, vztah, aktualni, ico, name, addr, d1, d2 };
        if (outRow.Count != 9) return (Array.Empty<string>(), false);
        return (outRow.ToArray(), true);
    }
}

// ======================================================
// Domain
// ======================================================
sealed class OwnershipRecord
{
    public long PCV; // BIGINT
    public int TypSubjektu;
    public int VztahKVozidlu;
    public bool Aktualni;
    public int? ICO;
    public string? Nazev;
    public string? Adresa;
    public DateTime? DatumOd;
    public DateTime? DatumDo;
}

static class OwnershipRecordParser
{
    private const string Layout = "dd.MM.yyyy";

    public static OwnershipRecord Parse(string[] f)
    {
        long pcv = long.Parse(f[0].Trim(), CultureInfo.InvariantCulture); // bounded to BIGINT
        int typ  = int.Parse(f[1].Trim(), CultureInfo.InvariantCulture);
        int vztah = int.Parse(f[2].Trim(), CultureInfo.InvariantCulture);
        bool aktualni = string.Equals(f[3].Trim(), "True", StringComparison.OrdinalIgnoreCase);

        int? ico = null;
        if (!string.IsNullOrWhiteSpace(f[4]) && int.TryParse(f[4].Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var icoVal)) ico = icoVal;

        string? nazev = string.IsNullOrWhiteSpace(f[5]) ? null : f[5].Trim();
        string? adresa = string.IsNullOrWhiteSpace(f[6]) ? null : f[6].Trim();

        DateTime? od = null, @do = null;
        if (!string.IsNullOrWhiteSpace(f[7]))
        {
            if (!DateTime.TryParseExact(f[7].Trim(), Layout, CultureInfo.InvariantCulture, DateTimeStyles.None, out var d))
                throw new FormatException("invalid Datum od");
            od = d;
        }
        if (!string.IsNullOrWhiteSpace(f[8]))
        {
            if (DateTime.TryParseExact(f[8].Trim(), Layout, CultureInfo.InvariantCulture, DateTimeStyles.None, out var d2))
                @do = d2;
        }

        return new OwnershipRecord
        {
            PCV = pcv,
            TypSubjektu = typ,
            VztahKVozidlu = vztah,
            Aktualni = aktualni,
            ICO = ico,
            Nazev = nazev,
            Adresa = adresa,
            DatumOd = od,
            DatumDo = @do
        };
    }
}

static class VehicleTechRow
{
    public static byte[] RowToJsonUtf8(string[] headers, string[] fields)
    {
        var dict = new Dictionary<string, string>(headers.Length, StringComparer.Ordinal);
        for (int i = 0; i < headers.Length && i < fields.Length; i++)
            dict[headers[i]] = fields[i];
        return JsonSerializer.SerializeToUtf8Bytes(dict);
    }

    public static int FindPcvIndex(string[] headers)
        => Array.FindIndex(headers, h => string.Equals(h.Trim(), "PČV", StringComparison.Ordinal));
}

// ======================================================
// Transient / Retry helpers
// ======================================================
static class Transient
{
    public static bool IsTransient(SqlException ex)
    {
        foreach (SqlError e in ex.Errors)
        {
            switch (e. Number)
            {
                case 4060: case 10928: case 10929: case 40197: case 40501:
                case 40613: case 49918: case 49919: case 49920:
                case 64: case 233: case 10053: case 10054: case 10060: case 11001:
                    return true;
            }
        }
        return false;
    }

    public static bool IsTransient(NpgsqlException ex)
        => ex.SqlState is "08006" or "08001" or "57P01" or "57P02" or "57P03";
}

static class Retry
{
    public static async Task InvokeAsync(Func<CancellationToken, Task> action, int attempts, TimeSpan baseDelay, SimpleLogger log, string op, CancellationToken ct)
    {
        int tryNo = 0;
        Exception? last = null;
        while (tryNo < attempts && !ct.IsCancellationRequested)
        {
            try
            {
                await action(ct).ConfigureAwait(false);
                return;
            }
            catch (SqlException ex) when (++tryNo < attempts && Transient.IsTransient(ex))
            {
                last = ex;
                var delay = TimeSpan.FromMilliseconds(baseDelay.TotalMilliseconds * Math.Pow(2, tryNo - 1));
                log.LogWarning("{0}: attempt {1}/{2} failed; retrying in {3}ms ({4})",
                    op, tryNo, attempts, (int)delay.TotalMilliseconds, ex.Message);
                await Task.Delay(delay, ct).ConfigureAwait(false);
            }
            catch (NpgsqlException ex) when (++tryNo < attempts && Transient.IsTransient(ex))
            {
                last = ex;
                var delay = TimeSpan.FromMilliseconds(baseDelay.TotalMilliseconds * Math.Pow(2, tryNo - 1));
                log.LogWarning("{0}: attempt {1}/{2} failed; retrying in {3}ms ({4})",
                    op, tryNo, attempts, (int)delay.TotalMilliseconds, ex.Message);
                await Task.Delay(delay, ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (++tryNo < attempts)
            {
                last = ex;
                var delay = TimeSpan.FromMilliseconds(baseDelay.TotalMilliseconds * Math.Pow(2, tryNo - 1));
                log.LogWarning("{0}: attempt {1}/{2} failed; retrying in {3}ms ({4})",
                    op, tryNo, attempts, (int)delay.TotalMilliseconds, ex.Message);
                await Task.Delay(delay, ct).ConfigureAwait(false);
            }
        }
        throw new InvalidOperationException($"{op} failed after {attempts} attempts", last);
    }
}

// ======================================================
// Adapters (DB specifics hidden here) + connection hygiene
// ======================================================
interface IOwnershipWriter : IAsyncDisposable
{
    Task EnsureTableAsync(bool pgUnlogged, CancellationToken ct);
    Task BulkWriteAsync(IEnumerable<OwnershipRecord> batch, CancellationToken ct);
}

interface IVehicleTechWriter : IAsyncDisposable
{
    Task EnsureTableAsync(bool pgUnlogged, CancellationToken ct);
    Task BulkWriteAsync(IEnumerable<(long pcv, ReadOnlyMemory<byte> jsonUtf8)> batch, CancellationToken ct);
}

// ---------- Postgres writers ----------
sealed class PgOwnershipWriter : IOwnershipWriter
{
    private readonly NpgsqlConnection _conn;
    private readonly SimpleLogger _log = LogNames.PgOwnershipWriter();

    public PgOwnershipWriter(string dsn) => _conn = new NpgsqlConnection(dsn);

    private async Task EnsureOpenAsync(CancellationToken ct)
    {
        if (_conn.State == ConnectionState.Open) return;
        await Retry.InvokeAsync(async _ => await _conn.OpenAsync(ct).ConfigureAwait(false),
            attempts: 5, baseDelay: TimeSpan.FromMilliseconds(150), _log, "pg.open", ct).ConfigureAwait(false);
    }

    public async Task EnsureTableAsync(bool pgUnlogged, CancellationToken ct)
    {
        await EnsureOpenAsync(ct).ConfigureAwait(false);

        await using (var cmd = new NpgsqlCommand("""
            CREATE TABLE IF NOT EXISTS ownership(
              pcv BIGINT,
              typ_subjektu INT,
              vztah_k_vozidlu INT,
              aktualni BOOLEAN,
              ico INT,
              nazev TEXT,
              adresa TEXT,
              datum_od DATE,
              datum_do DATE
            );
        """, _conn))
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

        if (pgUnlogged)
        {
            try
            {
                await using var cmd = new NpgsqlCommand("ALTER TABLE ownership SET UNLOGGED;", _conn);
                await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
            catch (Exception ex) { _log.LogDebug(ex, "UNLOGGED not set; ignoring"); }
        }
        await using (var idx = new NpgsqlCommand("CREATE INDEX IF NOT EXISTS ownership_pcv_idx ON ownership(pcv);", _conn))
            await idx.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public async Task BulkWriteAsync(IEnumerable<OwnershipRecord> batch, CancellationToken ct)
    {
        await EnsureOpenAsync(ct).ConfigureAwait(false);

        await using var importer = await _conn.BeginBinaryImportAsync(@"
            COPY ownership (pcv, typ_subjektu, vztah_k_vozidlu, aktualni, ico, nazev, adresa, datum_od, datum_do)
            FROM STDIN (FORMAT BINARY)", ct).ConfigureAwait(false);

        foreach (var r in batch)
        {
            await importer.StartRowAsync(ct).ConfigureAwait(false);
            await importer.WriteAsync(r.PCV, NpgsqlDbType.Bigint, ct).ConfigureAwait(false);
            await importer.WriteAsync(r.TypSubjektu, NpgsqlDbType.Integer, ct).ConfigureAwait(false);
            await importer.WriteAsync(r.VztahKVozidlu, NpgsqlDbType.Integer, ct).ConfigureAwait(false);
            await importer.WriteAsync(r.Aktualni, NpgsqlDbType.Boolean, ct).ConfigureAwait(false);
            if (r.ICO.HasValue) await importer.WriteAsync(r.ICO.Value, NpgsqlDbType.Integer, ct).ConfigureAwait(false); else await importer.WriteNullAsync(ct).ConfigureAwait(false);
            if (r.Nazev is not null) await importer.WriteAsync(r.Nazev, NpgsqlDbType.Text, ct).ConfigureAwait(false); else await importer.WriteNullAsync(ct).ConfigureAwait(false);
            if (r.Adresa is not null) await importer.WriteAsync(r.Adresa, NpgsqlDbType.Text, ct).ConfigureAwait(false); else await importer.WriteNullAsync(ct).ConfigureAwait(false);
            if (r.DatumOd.HasValue) await importer.WriteAsync(r.DatumOd.Value, NpgsqlDbType.Date, ct).ConfigureAwait(false); else await importer.WriteNullAsync(ct).ConfigureAwait(false);
            if (r.DatumDo.HasValue) await importer.WriteAsync(r.DatumDo.Value, NpgsqlDbType.Date, ct).ConfigureAwait(false); else await importer.WriteNullAsync(ct).ConfigureAwait(false);
        }
        await importer.CompleteAsync(ct).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        try { await _conn.CloseAsync().ConfigureAwait(false); } finally { await _conn.DisposeAsync().ConfigureAwait(false); }
    }
}

sealed class PgVehicleTechWriter : IVehicleTechWriter
{
    private readonly NpgsqlConnection _conn;
    private readonly SimpleLogger _log = LogNames.PgVehicleTechWriter();

    public PgVehicleTechWriter(string dsn) => _conn = new NpgsqlConnection(dsn);

    private async Task EnsureOpenAsync(CancellationToken ct)
    {
        if (_conn.State == ConnectionState.Open) return;
        await Retry.InvokeAsync(async _ => await _conn.OpenAsync(ct).ConfigureAwait(false),
            attempts: 5, baseDelay: TimeSpan.FromMilliseconds(150), _log, "pg.open", ct).ConfigureAwait(false);
    }

    public async Task EnsureTableAsync(bool pgUnlogged, CancellationToken ct)
    {
        await EnsureOpenAsync(ct).ConfigureAwait(false);

        await using (var cmd = new NpgsqlCommand("""
            CREATE TABLE IF NOT EXISTS vehicle_tech(
              pcv BIGINT,
              payload JSONB
            );
        """, _conn)) { await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false); }

        if (pgUnlogged)
        {
            try { await using var cmd = new NpgsqlCommand("ALTER TABLE vehicle_tech SET UNLOGGED;", _conn); await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false); }
            catch (Exception ex) { _log.LogDebug(ex, "UNLOGGED not set; ignoring"); }
        }
        await using (var idx = new NpgsqlCommand("CREATE INDEX IF NOT EXISTS vehicle_tech_pcv_idx ON vehicle_tech(pcv);", _conn))
            await idx.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public async Task BulkWriteAsync(IEnumerable<(long pcv, ReadOnlyMemory<byte> jsonUtf8)> batch, CancellationToken ct)
    {
        await EnsureOpenAsync(ct).ConfigureAwait(false);

        await using var importer = await _conn.BeginBinaryImportAsync(@"
            COPY vehicle_tech (pcv, payload) FROM STDIN (FORMAT BINARY)", ct).ConfigureAwait(false);

        foreach (var (pcv, json) in batch)
        {
            await importer.StartRowAsync(ct).ConfigureAwait(false);
            await importer.WriteAsync(pcv, NpgsqlDbType.Bigint, ct).ConfigureAwait(false);
            await importer.WriteAsync(json.ToArray(), NpgsqlDbType.Jsonb, ct).ConfigureAwait(false);
        }
        await importer.CompleteAsync(ct).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        try { await _conn.CloseAsync().ConfigureAwait(false); } finally { await _conn.DisposeAsync().ConfigureAwait(false); }
    }
}

// ---------- SQL Server writers ----------
sealed class SqlOwnershipWriter : IOwnershipWriter
{
    private readonly SqlConnection _conn;
    private readonly SimpleLogger _log = LogNames.SqlOwnershipWriter();

    public SqlOwnershipWriter(string dsn) => _conn = new SqlConnection(dsn);

    public async Task EnsureTableAsync(bool _, CancellationToken ct)
    {
        await _conn.OpenAsync(ct).ConfigureAwait(false);
        await using var cmd = new SqlCommand("""
            IF OBJECT_ID(N'ownership', N'U') IS NULL
            BEGIN
                CREATE TABLE ownership(
                    pcv BIGINT,
                    typ_subjektu INT,
                    vztah_k_vozidlu INT,
                    aktualni BIT,
                    ico INT,
                    nazev NVARCHAR(MAX),
                    adresa NVARCHAR(MAX),
                    datum_od DATE,
                    datum_do DATE
                );
            END
            -- Index deferred until after load for performance.
        """, _conn);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private async Task EnsureOpenAsync(CancellationToken ct)
    {
        if (_conn.State != ConnectionState.Open)
            await _conn.OpenAsync(ct).ConfigureAwait(false);
    }

    public async Task BulkWriteAsync(IEnumerable<OwnershipRecord> batch, CancellationToken ct)
    {
        await EnsureOpenAsync(ct).ConfigureAwait(false);

        using var bcp = new SqlBulkCopy(
            _conn,
            SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.UseInternalTransaction,
            externalTransaction: null)
        {
            DestinationTableName = "ownership",
            BatchSize = Math.Max(10_000, (batch as ICollection<OwnershipRecord>)?.Count ?? 10_000),
            BulkCopyTimeout = 0,
            EnableStreaming = true
        };

        bcp.ColumnMappings.Add(0, "pcv");
        bcp.ColumnMappings.Add(1, "typ_subjektu");
        bcp.ColumnMappings.Add(2, "vztah_k_vozidlu");
        bcp.ColumnMappings.Add(3, "aktualni");
        bcp.ColumnMappings.Add(4, "ico");
        bcp.ColumnMappings.Add(5, "nazev");
        bcp.ColumnMappings.Add(6, "adresa");
        bcp.ColumnMappings.Add(7, "datum_od");
        bcp.ColumnMappings.Add(8, "datum_do");

        using var reader = new OwnershipReader(batch);
        await bcp.WriteToServerAsync(reader, ct).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        await _conn.CloseAsync().ConfigureAwait(false);
        await _conn.DisposeAsync().ConfigureAwait(false);
    }

    private sealed class OwnershipReader : IDataReader
    {
        private readonly IEnumerator<OwnershipRecord> _e;
        public OwnershipReader(IEnumerable<OwnershipRecord> src) => _e = src.GetEnumerator();

        public bool Read() => _e.MoveNext();
        public int FieldCount => 9;
        public object GetValue(int i) => i switch
        {
            0 => _e.Current.PCV,
            1 => _e.Current.TypSubjektu,
            2 => _e.Current.VztahKVozidlu,
            3 => _e.Current.Aktualni,
            4 => (object?)_e.Current.ICO ?? DBNull.Value,
            5 => (object?)_e.Current.Nazev ?? DBNull.Value,
            6 => (object?)_e.Current.Adresa ?? DBNull.Value,
            7 => (object?)_e.Current.DatumOd ?? DBNull.Value,
            8 => (object?)_e.Current.DatumDo ?? DBNull.Value,
            _ => throw new IndexOutOfRangeException()
        };
        public bool IsDBNull(int i) => GetValue(i) is DBNull;
        public void Dispose() => _e.Dispose();
        public string GetName(int i) => i.ToString(CultureInfo.InvariantCulture);
        public string GetDataTypeName(int i) => GetFieldType(i).Name;
        public Type GetFieldType(int i) => GetValue(i) is DBNull ? typeof(object) : GetValue(i).GetType();
        public int GetValues(object[] values) { for (int i = 0; i < FieldCount; i++) values[i] = GetValue(i); return FieldCount; }
        public int Depth => 0; public bool IsClosed => false; public int RecordsAffected => -1;
        public void Close() { } public DataTable GetSchemaTable() => throw new NotSupportedException();
        public object this[int i] => GetValue(i); public object this[string name] => throw new NotSupportedException();
        public bool NextResult() => false;
        public bool GetBoolean(int i) => (bool)GetValue(i);
        public byte GetByte(int i) => (byte)GetValue(i);
        public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) => throw new NotSupportedException();
        public char GetChar(int i) => (char)GetValue(i);
        public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) => throw new NotSupportedException();
        public IDataReader GetData(int i) => throw new NotSupportedException();
        public DateTime GetDateTime(int i) => (DateTime)GetValue(i);
        public decimal GetDecimal(int i) => (decimal)GetValue(i);
        public double GetDouble(int i) => (double)GetValue(i);
        public float GetFloat(int i) => (float)GetValue(i);
        public Guid GetGuid(int i) => (Guid)GetValue(i);
        public short GetInt16(int i) => (short)GetValue(i);
        public int GetInt32(int i) => (int)GetValue(i);
        public long GetInt64(int i) => (long)GetValue(i);
        public string GetString(int i) => (string)GetValue(i)!;
        public int GetOrdinal(string name) => int.Parse(name, CultureInfo.InvariantCulture);
    }
}

sealed class SqlVehicleTechWriter : IVehicleTechWriter
{
    private readonly SqlConnection _conn;
    public SqlVehicleTechWriter(string dsn) => _conn = new SqlConnection(dsn);

    public async Task EnsureTableAsync(bool _, CancellationToken ct)
    {
        await _conn.OpenAsync(ct).ConfigureAwait(false);
        await using var cmd = new SqlCommand("""
            IF OBJECT_ID(N'vehicle_tech', N'U') IS NULL
            BEGIN
                CREATE TABLE vehicle_tech(
                    pcv BIGINT,
                    payload NVARCHAR(MAX) CHECK (ISJSON(payload)=1)
                );
            END
            -- Index deferred until after load for performance.
        """, _conn);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private async Task EnsureOpenAsync(CancellationToken ct)
    {
        if (_conn.State != ConnectionState.Open)
            await _conn.OpenAsync(ct).ConfigureAwait(false);
    }

    public async Task BulkWriteAsync(IEnumerable<(long pcv, ReadOnlyMemory<byte> jsonUtf8)> batch, CancellationToken ct)
    {
        await EnsureOpenAsync(ct).ConfigureAwait(false);

        using var bcp = new SqlBulkCopy(
            _conn,
            SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.UseInternalTransaction,
            externalTransaction: null)
        {
            DestinationTableName = "vehicle_tech",
            BatchSize = Math.Max(10_000, (batch as ICollection<(long, ReadOnlyMemory<byte>)>)?.Count ?? 10_000),
            BulkCopyTimeout = 0,
            EnableStreaming = true
        };

        bcp.ColumnMappings.Add(0, "pcv");
        bcp.ColumnMappings.Add(1, "payload");

        using var reader = new VehicleTechReader(batch);
        await bcp.WriteToServerAsync(reader, ct).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        await _conn.CloseAsync().ConfigureAwait(false);
        await _conn.DisposeAsync().ConfigureAwait(false);
    }

    private sealed class VehicleTechReader : IDataReader
    {
        private readonly IEnumerator<(long pcv, ReadOnlyMemory<byte> json)> _e;
        public VehicleTechReader(IEnumerable<(long pcv, ReadOnlyMemory<byte> json)> src) => _e = src.GetEnumerator();
        public bool Read() => _e.MoveNext();
        public int FieldCount => 2;
        public object GetValue(int i) => i switch
        {
            0 => _e.Current.pcv,
            1 => Encoding.UTF8.GetString(_e.Current.json.Span),
            _ => throw new IndexOutOfRangeException()
        };
        public void Dispose() => _e.Dispose();
        public bool IsDBNull(int i) => false;
        public string GetName(int i) => i.ToString(CultureInfo.InvariantCulture);
        public string GetDataTypeName(int i) => GetFieldType(i).Name;
        public Type GetFieldType(int i) => i == 0 ? typeof(long) : typeof(string);
        public int GetValues(object[] values) { values[0] = GetValue(0); values[1] = GetValue(1); return 2; }
        public int Depth => 0; public bool IsClosed => false; public int RecordsAffected => -1;
        public void Close() { } public DataTable GetSchemaTable() => throw new NotSupportedException();
        public object this[int i] => GetValue(i); public object this[string name] => throw new NotSupportedException();
        public bool NextResult() => false;
        public bool GetBoolean(int i) => (bool)GetValue(i);
        public byte GetByte(int i) => (byte)GetValue(i);
        public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) => throw new NotSupportedException();
        public char GetChar(int i) => (char)GetValue(i);
        public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) => throw new NotSupportedException();
        public IDataReader GetData(int i) => throw new NotSupportedException();
        public DateTime GetDateTime(int i) => (DateTime)GetValue(i);
        public decimal GetDecimal(int i) => (decimal)GetValue(i);
        public double GetDouble(int i) => (double)GetValue(i);
        public float GetFloat(int i) => (float)GetValue(i);
        public Guid GetGuid(int i) => (Guid)GetValue(i);
        public short GetInt16(int i) => (short)GetValue(i);
        public int GetInt32(int i) => (int)GetValue(i);
        public long GetInt64(int i) => (long)GetValue(i);
        public string GetString(int i) => (string)GetValue(i)!;
        public int GetOrdinal(string name) => int.Parse(name, CultureInfo.InvariantCulture);
    }
}

// ======================================================
// Factories
// ======================================================
interface IWriterFactory<T> where T : IAsyncDisposable
{
    ValueTask<T> CreateAsync(CancellationToken ct);
}

sealed class OwnershipWriterFactory : IWriterFactory<IOwnershipWriter>
{
    private readonly string _driver, _dsn;
    public OwnershipWriterFactory(string driver, string dsn) { _driver = driver; _dsn = dsn; }
    public async ValueTask<IOwnershipWriter> CreateAsync(CancellationToken ct)
        => _driver.Equals("postgres", StringComparison.OrdinalIgnoreCase)
            ? new PgOwnershipWriter(_dsn)
            : new SqlOwnershipWriter(_dsn);
}

sealed class VehicleTechWriterFactory : IWriterFactory<IVehicleTechWriter>
{
    private readonly string _driver, _dsn;
    public VehicleTechWriterFactory(string driver, string dsn) { _driver = driver; _dsn = dsn; }
    public async ValueTask<IVehicleTechWriter> CreateAsync(CancellationToken ct)
        => _driver.Equals("postgres", StringComparison.OrdinalIgnoreCase)
            ? new PgVehicleTechWriter(_dsn)
            : new SqlVehicleTechWriter(_dsn);
}

// ======================================================
// Pipelines (Channels, batching + time-based flush, structured logs)
// ======================================================
static class OwnershipPipeline
{
    private static readonly SimpleLogger _log = LogNames.OwnershipPipeline();
    private const int ChannelCapacity = 32_768;

    public static async Task RunAsync(Config cfg, IWriterFactory<IOwnershipWriter> factory, CancellationToken ct)
    {
        Directory.CreateDirectory(cfg.SkippedDir);
        var ch = Channel.CreateBounded<(string line, int lineNum)>(new BoundedChannelOptions(ChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait
        });

        // Reader (producer)
        var readerTask = Task.Run(async () =>
        {
            using var fs = new FileStream(cfg.OwnershipCSV, FileMode.Open, FileAccess.Read, FileShare.Read,
                                          bufferSize: 4 << 20, FileOptions.SequentialScan);
            using var sr = new StreamReader(fs, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 4 << 20, leaveOpen: false);

            _ = await Csv.ReadLogicalCsvLineAsync(sr, ct).ConfigureAwait(false); // header

            int ln = 1;
            while (true)
            {
                var l = await Csv.ReadLogicalCsvLineAsync(sr, ct).ConfigureAwait(false);
                if (l is null) break;
                ln++;
                await ch.Writer.WriteAsync((l, ln), ct).ConfigureAwait(false);
            }
            ch.Writer.Complete();
        }, ct);

        // Ensure table once (control instance)
        await using (var writer = await factory.CreateAsync(ct).ConfigureAwait(false))
            await writer.EnsureTableAsync(pgUnlogged: cfg.UnloggedTables, ct).ConfigureAwait(false);

        // Workers
        var workerTasks = Enumerable.Range(0, Math.Max(1, cfg.OwnershipWorkers))
            .Select(id => Task.Run(() => WorkerAsync(id + 1, cfg, factory, ch.Reader, ct), ct))
            .ToArray();

        _log.LogInformation("ownership: starting {Workers} workers, batch={Batch}, flushMs={Flush}", cfg.OwnershipWorkers, cfg.BatchSize, cfg.FlushMs);

        await Task.WhenAll(workerTasks.Append(readerTask)).ConfigureAwait(false);
        _log.LogInformation("ownership (parallel {Workers}) complete", cfg.OwnershipWorkers);
    }

    private static async Task WorkerAsync(
        int workerId,
        Config cfg,
        IWriterFactory<IOwnershipWriter> factory,
        ChannelReader<(string line, int lineNum)> reader,
        CancellationToken ct)
    {
        var log = LogNames.OwnershipPipeline();
        var skippedPath = Path.Combine(cfg.SkippedDir, $"skipped_ownership_w{workerId}.csv");
        await using var sk = new StreamWriter(new FileStream(skippedPath, FileMode.Create, FileAccess.Write, FileShare.Read), Encoding.UTF8);
        await sk.WriteLineAsync("reason,line_number,pcv_field,raw_line").ConfigureAwait(false);

        var batch = new List<OwnershipRecord>(cfg.BatchSize);
        int inserted = 0, skipped = 0;

        await using var writer = await factory.CreateAsync(ct).ConfigureAwait(false);
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(Math.Max(50, cfg.FlushMs)));

        async Task FlushAsync()
        {
            if (batch.Count == 0) return;
            var local = batch.ToArray();
            batch.Clear();

            await Retry.InvokeAsync(async _ =>
            {
                await writer.BulkWriteAsync(local, ct).ConfigureAwait(false);
            }, attempts: 3, baseDelay: TimeSpan.FromMilliseconds(200), log, $"ownership[w{workerId}].flush({local.Length})", ct).ConfigureAwait(false);

            inserted += local.Length;
            log.LogInformation("ownership[w{Worker}] inserted={Inserted} skipped={Skipped}", workerId, inserted, skipped);
        }

        const int MaxBurst = 4096;

        while (!ct.IsCancellationRequested)
        {
            var readTask = reader.WaitToReadAsync(ct).AsTask();
            var tickTask = timer.WaitForNextTickAsync(ct).AsTask();

            var completed = await Task.WhenAny(readTask, tickTask).ConfigureAwait(false);

            if (completed == readTask && await readTask.ConfigureAwait(false))
            {
                int processed = 0;
                while (reader.TryRead(out var job))
                {
                    string[] fields;
                    try { fields = Csv.ParseCsvLineLoose(job.line); }
                    catch
                    {
                        await sk.WriteLineAsync($"parse_error,{job.lineNum},,\"{job.line}\"").ConfigureAwait(false);
                        skipped++; continue;
                    }

                    if (fields.Length != 9)
                    {
                        var (rep1, ok1) = Csv.RepairOverlongCommaFields(fields);
                        if (ok1) fields = rep1;
                        else
                        {
                            var (rep2, ok2) = Csv.ParseSpaceSeparatedRow(job.line);
                            if (ok2) fields = rep2;
                            else { await sk.WriteLineAsync($"column_mismatch,{job.lineNum},,\"{job.line}\"").ConfigureAwait(false); skipped++; continue; }
                        }
                    }

                    try
                    {
                        var rec = OwnershipRecordParser.Parse(fields);
                        batch.Add(rec);
                        if (batch.Count >= cfg.BatchSize) await FlushAsync().ConfigureAwait(false);
                    }
                    catch
                    {
                        await sk.WriteLineAsync($"field_parse_error,{job.lineNum},\"{fields.ElementAtOrDefault(0)}\",\"{job.line}\"").ConfigureAwait(false);
                        skipped++;
                    }

                    if (++processed >= MaxBurst) break;
                }
            }
            else if (completed == tickTask && await tickTask.ConfigureAwait(false))
            {
                await FlushAsync().ConfigureAwait(false);
            }

            if (completed == readTask && !await readTask.ConfigureAwait(false))
                break;
        }

        await FlushAsync().ConfigureAwait(false);
    }
}

// =======================
// VehicleTechPipeline
// =======================
static class VehicleTechPipeline
{
    private static readonly SimpleLogger _log = LogNames.VehicleTechPipeline();
    private const int ChannelCapacity = 32_768;

    public static async Task RunAsync(
        Config cfg,
        string driver,
        string dsn,
        IWriterFactory<IVehicleTechWriter> factory,
        CancellationToken ct)
    {
        Directory.CreateDirectory(cfg.SkippedDir);

        // Parse header up-front
        string[] headers;
        int pcvIdx, statusIdx;

        using var fs = new FileStream(cfg.VehicleCSV, FileMode.Open, FileAccess.Read, FileShare.Read,
                                      bufferSize: 4 << 20, FileOptions.SequentialScan);
        using var sr = new StreamReader(fs, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 4 << 20, leaveOpen: true);

        var headerLine = await Csv.ReadLogicalCsvLineAsync(sr, ct).ConfigureAwait(false)
                         ?? throw new InvalidOperationException("empty file");
        headers   = Csv.ParseCsvLineLoose(headerLine);
        pcvIdx    = VehicleTechRow.FindPcvIndex(headers);
        if (pcvIdx < 0) throw new InvalidOperationException("PČV column not found in header");
        statusIdx = Array.FindIndex(headers, h => string.Equals(h.Trim(), "Status", StringComparison.Ordinal));

        var ch = Channel.CreateBounded<(string line, int lineNum)>(
            new BoundedChannelOptions(ChannelCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = true,
                SingleReader = false
            });

        // Reader
        var readerTask = Task.Run(async () =>
        {
            int ln = 1;
            while (true)
            {
                var l = await Csv.ReadLogicalCsvLineAsync(sr, ct).ConfigureAwait(false);
                if (l is null) break;
                ln++;
                await ch.Writer.WriteAsync((l, ln), ct).ConfigureAwait(false);
            }
            ch.Writer.Complete();
        }, ct);

        // Ensure table once
        await using (var w = await factory.CreateAsync(ct).ConfigureAwait(false))
            await w.EnsureTableAsync(pgUnlogged: cfg.UnloggedTables, ct).ConfigureAwait(false);

        // Workers
        var workers = Enumerable.Range(0, Math.Max(1, cfg.TechWorkers))
            .Select(i => Task.Run(
                () => WorkerAsync(i + 1, cfg, headers, pcvIdx, statusIdx, factory, ch.Reader, ct), ct))
            .ToArray();

        _log.LogInformation("vehicle_tech: starting {Workers} workers, batch={Batch}, flushMs={Flush}", cfg.TechWorkers, cfg.BatchSize, cfg.FlushMs);

        await Task.WhenAll(workers.Append(readerTask)).ConfigureAwait(false);
        _log.LogInformation("vehicle_tech (parallel {Workers}) complete", cfg.TechWorkers);
    }

    private static async Task WorkerAsync(
        int workerId,
        Config cfg,
        string[] headers,
        int pcvIdx,
        int statusIdx,
        IWriterFactory<IVehicleTechWriter> factory,
        ChannelReader<(string line, int lineNum)> reader,
        CancellationToken ct)
    {
        var log = LogNames.VehicleTechPipeline();
        var skippedPath = Path.Combine(cfg.SkippedDir, $"skipped_vehicle_tech_w{workerId}.csv");
        await using var sk = new StreamWriter(new FileStream(skippedPath, FileMode.Create, FileAccess.Write, FileShare.Read), Encoding.UTF8);
        await sk.WriteLineAsync("reason,line_number,pcv_field,raw_line").ConfigureAwait(false);

        var batch = new List<(long pcv, ReadOnlyMemory<byte> jsonUtf8)>(cfg.BatchSize);
        int inserted = 0, skipped = 0;
        var digitsOnly = new Regex(@"^\d+$", RegexOptions.Compiled);

        await using var writer = await factory.CreateAsync(ct).ConfigureAwait(false);
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(Math.Max(50, cfg.FlushMs)));

        static (long, string) ExtractPCV(string[] headers, string[] fields, int pcvIdx, int statusIdx, Regex reDigits)
        {
            static bool Num(Regex re, string s) => re.IsMatch(s);

            if (pcvIdx >= 0 && pcvIdx < fields.Length)
            {
                var s = fields[pcvIdx].Trim();
                if (Num(reDigits, s) && long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v)) return (v, s);
            }

            if (fields.Length != headers.Length && pcvIdx >= 0)
            {
                int tailOffset = (headers.Length - 1) - pcvIdx;
                int idx = (fields.Length - 1) - tailOffset;
                if (idx >= 0 && idx < fields.Length)
                {
                    var s = fields[idx].Trim();
                    if (Num(reDigits, s) && long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v)) return (v, s);
                }
            }

            if (statusIdx >= 0)
            {
                (long, string, bool) TryFrom(int si)
                {
                    if (si < 0 || si >= fields.Length) return (0, "", false);
                    for (int i = si + 1; i < fields.Length && i <= si + 10; i++)
                    {
                        var s = fields[i].Trim();
                        if (Num(reDigits, s) && s.Length >= 5 && long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v)) return (v, s, true);
                    }
                    return (0, "", false);
                }
                var t1 = TryFrom(statusIdx);
                if (t1.Item3) return (t1.Item1, t1.Item2);
                if (fields.Length != headers.Length)
                {
                    int tailOffsetS = (headers.Length - 1) - statusIdx;
                    int si = (fields.Length - 1) - tailOffsetS;
                    var t2 = TryFrom(si);
                    if (t2.Item3) return (t2.Item1, t2.Item2);
                }
            }

            int start = Math.Max(0, fields.Length - 16);
            for (int i = fields.Length - 1; i >= start; i--)
            {
                var s = fields[i].Trim();
                if (Num(reDigits, s) && s.Length >= 6 && long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v)) return (v, s);
            }
            return (0, "");
        }

        async Task FlushAsync()
        {
            if (batch.Count == 0) return;
            var local = batch.ToArray();
            batch.Clear();

            await Retry.InvokeAsync(async _ =>
            {
                await writer.BulkWriteAsync(local, ct).ConfigureAwait(false);
            }, attempts: 3, baseDelay: TimeSpan.FromMilliseconds(200), log, $"vehicle_tech[w{workerId}].flush({local.Length})", ct).ConfigureAwait(false);

            inserted += local.Length;
            log.LogInformation("vehicle_tech[w{Worker}] inserted={Inserted} skipped={Skipped}", workerId, inserted, skipped);
        }

        const int MaxBurst = 4096;

        while (!ct.IsCancellationRequested)
        {
            var readTask = reader.WaitToReadAsync(ct).AsTask();
            var tickTask = timer.WaitForNextTickAsync(ct).AsTask();

            var completed = await Task.WhenAny(readTask, tickTask).ConfigureAwait(false);

            if (completed == readTask && await readTask.ConfigureAwait(false))
            {
                int processed = 0;
                while (reader.TryRead(out var job))
                {
                    string[] fields;
                    try { fields = Csv.ParseCsvLineLoose(job.line); }
                    catch
                    {
                        await sk.WriteLineAsync($"parse_error,{job.lineNum},,\"{job.line}\"").ConfigureAwait(false);
                        skipped++; continue;
                    }

                    var (pcv, pcvStr) = ExtractPCV(headers, fields, pcvIdx, statusIdx, digitsOnly);
                    if (pcv == 0)
                    {
                        await sk.WriteLineAsync($"pcv_not_numeric,{job.lineNum},\"{pcvStr}\",\"{job.line}\"").ConfigureAwait(false);
                        skipped++; continue;
                    }

                    if (fields.Length > headers.Length) fields = fields.Take(headers.Length).ToArray();
                    else if (fields.Length < headers.Length) fields = fields.Concat(Enumerable.Repeat("", headers.Length - fields.Length)).ToArray();

                    byte[] json = VehicleTechRow.RowToJsonUtf8(headers, fields);
                    batch.Add((pcv, json));

                    if (batch.Count >= cfg.BatchSize) await FlushAsync().ConfigureAwait(false);
                    if (++processed >= MaxBurst) break;
                }
            }
            else if (completed == tickTask && await tickTask.ConfigureAwait(false))
            {
                await FlushAsync().ConfigureAwait(false);
            }

            if (completed == readTask && !await readTask.ConfigureAwait(false))
                break;
        }

        await FlushAsync().ConfigureAwait(false);
    }
}

// ======================================================
// Program
// ======================================================
class Program
{
    static async Task<int> Main(string[] args)
    {
        var cfg = new Config();
        cfg.ApplyArgs(args);

        var log = LogNames.Program();
        await Task.Delay(TimeSpan.FromSeconds(2)).ConfigureAwait(false);

        string driver = cfg.DBDriver.ToLowerInvariant();
        string dsn = cfg.DSN;
        if (driver == "postgres" && string.IsNullOrWhiteSpace(dsn))
            dsn = cfg.BuildPgDsn();
        if (driver == "mssql" && string.IsNullOrWhiteSpace(dsn))
        {
            Console.Error.WriteLine("For --db-driver=mssql please provide --dsn (SqlClient connection string).");
            return 2;
        }

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

        try
        {
            var ownershipFactory = new OwnershipWriterFactory(driver, dsn);
            var techFactory      = new VehicleTechWriterFactory(driver, dsn);

            var t1 = OwnershipPipeline.RunAsync(cfg, ownershipFactory, cts.Token);
            var t2 = VehicleTechPipeline.RunAsync(cfg, driver, dsn, techFactory, cts.Token);

            var sw = System.Diagnostics.Stopwatch.StartNew();
            await Task.WhenAll(t1, t2).ConfigureAwait(false);
            sw.Stop();
            log.LogInformation("✅ All imports complete in {ElapsedMs} ms", sw.ElapsedMilliseconds);

            // Post-load index creation for SQL Server to avoid contention during load.
            if (driver == "mssql")
            {
                log.LogInformation("Creating SQL Server indexes post-load...");
                using var post = new SqlConnection(dsn);
                await post.OpenAsync(cts.Token).ConfigureAwait(false);

                using (var cmd = new SqlCommand("IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='ownership_pcv_idx') CREATE INDEX ownership_pcv_idx ON ownership(pcv);", post))
                    await cmd.ExecuteNonQueryAsync(cts.Token).ConfigureAwait(false);
                using (var cmd = new SqlCommand("IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='vehicle_tech_pcv_idx') CREATE INDEX vehicle_tech_pcv_idx ON vehicle_tech(pcv);", post))
                    await cmd.ExecuteNonQueryAsync(cts.Token).ConfigureAwait(false);

                log.LogInformation("SQL Server indexes created.");
            }

            return 0;
        }
        catch (OperationCanceledException)
        {
            Console.Error.WriteLine("Canceled.");
            return 130;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"ERROR: {ex}");
            return 1;
        }
        finally { }
    }
}

