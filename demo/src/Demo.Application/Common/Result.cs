namespace Demo.Application.Common;

/// <summary>Either a value or an <see cref="AppError"/>.</summary>
public sealed class Result<T>
{
    private readonly T? _value;
    private readonly AppError? _error;

    private Result(T? value, AppError? error)
    {
        _value = value;
        _error = error;
    }

    public bool IsSuccess => _error is null;

    public bool IsFailure => _error is not null;

    /// <summary>The value. Throws when the result is a failure.</summary>
    public T Value => _error is null
        ? _value!
        : throw new InvalidOperationException("A failed result has no value.");

    /// <summary>The error. Throws when the result is a success.</summary>
    public AppError Error => _error
        ?? throw new InvalidOperationException("A successful result has no error.");

    public static Result<T> Success(T value) => new(value, null);

    public static Result<T> Failure(AppError error)
    {
        ArgumentNullException.ThrowIfNull(error);
        return new Result<T>(default, error);
    }

    public static implicit operator Result<T>(AppError error) => Failure(error);
}
