# Repository Guidelines

## Project Structure & Module Organization

This is a .NET 10 solution centered on `Eventa.slnx`. The `src/Eventa.csproj` project publishes the `Eventa` package, with core library code in `src/Eventa` and the System.Threading.Channels adapter in `src/Eventa.Adapters/Channels`. Tests share the `tests/Eventa.Tests.csproj` project and mirror that module layout under `tests`. `examples/Eventa.Example` is the console sample used to validate public API ergonomics. Design notes and RFCs belong in `docs`; generated build output belongs in `artifacts` and should not be edited by hand.

## Build, Test, and Development Commands

Use the .NET 10 SDK. Common commands:

```text
dotnet restore Eventa.slnx --locked-mode
dotnet build Eventa.slnx --configuration Release --no-restore
dotnet test --project tests/Eventa.Tests.csproj --configuration Release --no-build
dotnet run --project examples/Eventa.Example/Eventa.Example.csproj --configuration Release --no-restore
```

Restore first, build the full solution, then run the xUnit test project. This repository uses Microsoft.Testing.Platform, so agents should not rely on repo-wide `dotnet test` discovery when narrowing scope. Run the example after API or behavior changes that affect user-facing flows.

## Coding Style & Naming Conventions

Follow `.editorconfig`: spaces only, 4-space indentation for C#, 2-space indentation for project/XML/JSON files. C# uses nullable reference types and implicit usings. Prefer explicit types over `var`, file-scoped namespaces, braces for blocks, sorted `System` usings first, and static local functions where practical. Public types, methods, and properties use PascalCase; interfaces use `I` + PascalCase; type parameters use `T` + PascalCase.

## Testing Guidelines

Tests use xUnit v3 with Microsoft.Testing.Platform, configured by `global.json`. Always pass `--project` when invoking tests from the CLI in this repo. For targeted validation, use Microsoft.Testing.Platform/xUnit v3 switches such as `--filter-class Eventa.Tests.AsyncSignalQueueTests`, and prefer fully qualified class names even when the simple class name appears unique.

```text
dotnet test --project tests/Eventa.Tests.csproj --configuration Release --no-build --filter-class Eventa.Tests.AsyncSignalQueueTests
```

Do not use VSTest-style `--filter "..."` expressions here; they are the wrong syntax for this setup and will commonly return zero tests. Filtering by class is the most reliable narrow validation path in this repo. Keep tests close to the behavior they cover and name methods in the existing `MethodOrScenario_Condition_ExpectedResult` style, for example `InvokeAsync_WithPreCanceledToken_EmitsAbortOnlyOnce`. Add regression coverage for cancellation, streaming, adapter faulting, and concurrency changes.

## Commit & Pull Request Guidelines

History uses Conventional Commit prefixes such as `feat:`, `fix:`, `docs:`, `chore:`, and scoped forms like `feat(example):`. Keep commits focused and imperative. Pull requests should describe the behavior change, list validation commands run, link related issues or RFCs, and call out public API or protocol compatibility impact.

## Agent-Specific Notes

Keep plans concise and list unresolved questions at the end. When sharing runnable PowerShell commands, prefer fenced `text` blocks rather than inline command formatting. For targeted validation, prefer explicit `dotnet test --project ...` commands and `--filter-class <FullyQualifiedClassName>` over generic single-test tooling or VSTest filter expressions.
