namespace Demo.SalesLeads.Application;

/// <summary>Generates mock customer data for a quote (Req 6.3).</summary>
public interface IMockDataGenerator
{
    /// <summary>Customer name, 1 to 100 characters.</summary>
    string GenerateName();

    /// <summary>Valid email address, 3 to 254 characters.</summary>
    string GenerateEmail();

    /// <summary>Amount from 0.01 to 999,999.99 with two decimal places.</summary>
    decimal GenerateAmount();
}
