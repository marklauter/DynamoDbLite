# Build runnable sample projects for wiki recipes to reference

Tags: todo,wiki,recipes,samples
Wiki recipes currently inline code blocks that drift from the source and have not been compile-verified; sample projects in the code repo (parallel to plumber-wiki-samples) become the source of truth and wiki recipes link to them.

## Observation

The four DynamoDbLite wiki recipes — `Recipe-DynamoDbContext-For-Tests`, `Recipe-Xunit-Per-Test-Isolation`, `Recipe-Aspnet-Integration-Test-Fixture`, `Recipe-Migrate-From-Dynamodb-Local` — and the `Tutorial` carry their code as fenced markdown blocks. None have been compiled against the current `DynamoDbLite` package. The plan's verification step ("paste new code samples into a scratch project") was skipped.

Inline-code recipes have two structural problems:

1. **Drift on API change.** When `DynamoDbLiteOptions` changed shape mid-audit (parameterless constructor and default `ConnectionString` removed), every wiki snippet showing `new DynamoDbClient()` silently broke. The audit caught it; without an audit, drift accumulates between releases.
2. **No live verification.** A reader who copy-pastes a snippet has no signal that the snippet ran end-to-end on a current build. The reader's compiler is the first verifier.

Plumber's wiki has a `plumber-wiki-samples/` directory at the root of the wiki repo containing runnable projects for each recipe. The recipe pages reference the sample by file and line, and the build verifies the sample compiles before the wiki ships.

## Interpretation

The plumber pattern shifts the source of truth: the sample project is canonical, the wiki page is documentation about the sample. Editorial gains:

- Code samples in the wiki shrink to the load-bearing fragment, not the whole working program. The supporting infrastructure (project file, `Program.cs`, fixtures) lives in the sample where it can compile.
- A recipe links to the sample (`samples/DynamoDbContextForTests/`) for the reader who wants to clone-and-run; the wiki prose explains the *why* and the interesting wrinkles.
- CI can include a `dotnet build` step over the samples directory. Any wiki-relevant API change breaks the build, surfaces immediately, and is fixed in the same PR as the API change.

Open question on placement: samples in a `samples/` directory inside the **code repo** (visible to `dotnet build`, sits beside `src/` and `tests/`) versus a `dynamodblite-wiki-samples/` directory in the **wiki repo** (matches plumber's layout, but harder to wire into the main solution's build). Plumber chose the latter. The DynamoDbLite layout — code and wiki as sibling repos under `D:/dynamodblite/` — makes either viable.

## Next

Sequenced:

1. **Pick the samples location** — `samples/` in the code repo (easier CI) or `dynamodblite-wiki-samples/` in the wiki repo (matches plumber). Decision belongs to the project owner.
2. **Build one sample first** — `DynamoDbContextForTests` is the highest-value recipe (zero current wiki coverage, real undocumented capability). Get the pattern right on one before propagating.
3. **Wire it into the build** — `dotnet build` over the samples directory, ideally inside the main solution so a single `dotnet build` covers the lot.
4. **Refactor the recipe page** — strip inline code to the load-bearing fragments, add a "Full sample" link to the project directory.
5. **Repeat for the remaining three recipes and the Tutorial.**
