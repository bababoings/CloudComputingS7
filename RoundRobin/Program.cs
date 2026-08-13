// The Dining Philosophers Problem — Concurrent Round Robin
// 5 philosophers run on 5 threads. Forks are real locks. The right to eat is
// granted one philosopher at a time in round-robin order, which makes the
// simulation deadlock-free and starvation-free.

Console.WriteLine("The Dining Philosophers Problem — Concurrent Round Robin");
Console.WriteLine("5 philosophers on 5 threads, forks are locks, eating turns are round-robin.\n");

var table = new DiningTable(seats: 5, mealsPerPhilosopher: 3);
table.Dine();


/// <summary>
/// Thread-safe console writer. Several philosopher threads print at once, so we
/// serialise writes through a single lock to keep lines from interleaving.
/// </summary>
static class Log
{
    private static readonly object _consoleLock = new();

    public static void Line(string message)
    {
        lock (_consoleLock)
        {
            Console.WriteLine(message);
        }
    }
}

/// <summary>
/// A fork shared between two neighbouring philosophers. It wraps a
/// SemaphoreSlim with a single permit, acting as a real mutual-exclusion lock:
/// only ONE philosopher can hold the fork at a time; a second one blocks on
/// <see cref="PickUp"/> until it is put back down.
/// </summary>
class Fork
{
    private readonly SemaphoreSlim _lock = new(initialCount: 1, maxCount: 1);

    public int Id { get; }
    public string? Owner { get; private set; }

    public Fork(int id) => Id = id;

    public void PickUp(string owner)
    {
        _lock.Wait();
        Owner = owner;
    }

    public void PutDown()
    {
        Owner = null;
        _lock.Release();
    }
}

/// <summary>
/// Coordinates EATING with a ROUND ROBIN policy. All philosophers think in
/// parallel, but the right to eat is granted to one philosopher at a time in a
/// fixed circular order (0 → 1 → 2 → 3 → 4 → 0 → ...). This guarantees no
/// deadlock (neighbours never grab the same fork at once) and no starvation
/// (everyone is served once per round). Implemented with a Monitor
/// (lock + Wait/Pulse) — the classic condition-variable pattern.
/// </summary>
class RoundRobinScheduler
{
    private readonly int _count;
    private readonly object _gate = new();
    private int _currentTurn;

    public RoundRobinScheduler(int count) => _count = count;

    /// <summary>Blocks the calling philosopher's thread until it is their turn to eat.</summary>
    public void WaitForTurn(int philosopherId)
    {
        lock (_gate)
        {
            while (_currentTurn != philosopherId)
            {
                Monitor.Wait(_gate);
            }
        }
    }

    /// <summary>Passes the turn to the next philosopher and wakes the waiting threads.</summary>
    public void EndTurn()
    {
        lock (_gate)
        {
            _currentTurn = (_currentTurn + 1) % _count;
            Monitor.PulseAll(_gate);
        }
    }
}

/// <summary>
/// A philosopher who runs on their OWN THREAD, looping between thinking and
/// eating. Thinking runs concurrently with everyone else; to eat they wait for
/// their round-robin turn, then pick up the left and right forks (both locks).
/// </summary>
class Philosopher
{
    private readonly RoundRobinScheduler _scheduler;
    private readonly int _meals;
    private readonly Random _random;

    public int Id { get; }
    public string Name { get; }
    public Fork LeftFork { get; }
    public Fork RightFork { get; }
    public int TimesEaten { get; private set; }

    public Philosopher(int id, string name, Fork leftFork, Fork rightFork,
                       RoundRobinScheduler scheduler, int meals)
    {
        Id = id;
        Name = name;
        LeftFork = leftFork;
        RightFork = rightFork;
        _scheduler = scheduler;
        _meals = meals;
        _random = new Random(id * 100 + 7);   // per-thread Random (Random is not thread-safe)
    }

    /// <summary>The thread body: alternate thinking and eating until fed.</summary>
    public void Run()
    {
        for (int i = 0; i < _meals; i++)
        {
            Think();
            _scheduler.WaitForTurn(Id);   // round-robin gate — blocks until it's our turn
            Eat();
            _scheduler.EndTurn();         // hand the turn to the next philosopher
        }
        Log.Line($"[{Name}] is full and leaves the table.");
    }

    private void Think()
    {
        Log.Line($"[{Name}] is thinking...");
        Thread.Sleep(_random.Next(100, 400));   // thinking takes a while (runs in parallel)
    }

    private void Eat()
    {
        LeftFork.PickUp(Name);
        Log.Line($"[{Name}] picked up LEFT fork #{LeftFork.Id}");

        RightFork.PickUp(Name);
        Log.Line($"[{Name}] picked up RIGHT fork #{RightFork.Id}");

        TimesEaten++;
        Log.Line($"[{Name}] is EATING (meal #{TimesEaten})");
        Thread.Sleep(_random.Next(150, 300));

        RightFork.PutDown();
        LeftFork.PutDown();
        Log.Line($"[{Name}] put down both forks");
    }
}

/// <summary>
/// The round table. Builds the forks, philosophers and the round-robin
/// scheduler, then starts one THREAD per philosopher so they run concurrently,
/// and joins them before printing a summary.
/// </summary>
class DiningTable
{
    private readonly List<Philosopher> _philosophers = new();
    private readonly List<Fork> _forks = new();

    public DiningTable(int seats, int mealsPerPhilosopher)
    {
        // One fork per seat, arranged in a circle.
        for (int i = 0; i < seats; i++)
        {
            _forks.Add(new Fork(i));
        }

        var scheduler = new RoundRobinScheduler(seats);
        string[] names = { "Socrates", "Plato", "Aristotle", "Kant", "Descartes" };

        for (int i = 0; i < seats; i++)
        {
            Fork left = _forks[i];
            Fork right = _forks[(i + 1) % seats];   // fork shared with the next philosopher
            string name = names[i % names.Length];
            _philosophers.Add(new Philosopher(i, name, left, right, scheduler, mealsPerPhilosopher));
        }
    }

    public void Dine()
    {
        // Start each philosopher on their own thread — they now run in parallel.
        var threads = new List<Thread>();
        foreach (Philosopher philosopher in _philosophers)
        {
            var thread = new Thread(philosopher.Run) { Name = philosopher.Name };
            threads.Add(thread);
            thread.Start();
        }

        // Wait for everyone to finish eating.
        foreach (Thread thread in threads)
        {
            thread.Join();
        }

        PrintSummary();
    }

    private void PrintSummary()
    {
        Log.Line("");
        Log.Line("===== Summary =====");
        foreach (Philosopher philosopher in _philosophers)
        {
            Log.Line($"  {philosopher.Name} ate {philosopher.TimesEaten} time(s).");
        }
    }
}
