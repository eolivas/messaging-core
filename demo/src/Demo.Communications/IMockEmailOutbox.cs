namespace Demo.Communications;

/// <summary>In-memory record of mock emails, bounded so a long demo cannot grow without limit.</summary>
public interface IMockEmailOutbox
{
    /// <summary>Adds an email, evicting the oldest entry when the bound is reached.</summary>
    void Add(MockEmail email);

    /// <summary>Returns a copy of the stored emails, oldest first.</summary>
    IReadOnlyList<MockEmail> Snapshot();
}
