# Reproducible user evidence

Independent experience has to come from people who actually used Bagel. This kit
is draft material for inviting and recording that experience. No testimonial,
review, endorsement, message delivery, or third-party listing change is implied.

## A useful report

Use the repository's **Workflow report** issue template for a success, partial
result, or failure. Capture:

- Task, exact prompt, client/model/version, Bagel commit or image digest, OS and runtime.
- A shareable sample and its provenance/license, or a synthetic reproduction.
- Copyable commands, schema and unit assumptions, SQL, and preview parameters.
- Actual outcome: detected events, retained duration, input/output byte sizes,
  elapsed time and hardware when relevant, plus validation and limitations.
- Screenshots or video accompanied by searchable text and underlying evidence.
- Whether the author works on Bagel or received assistance/incentives.

A maintainer reproduction is useful evidence but is not an independent user review.
Let users write their own conclusions, including failures. Never condition help on
a positive rating, invent reviews, or ask agents to create endorsements as users.

## Invitation draft — not sent

> We're looking for people who analyze ROS/MCAP or PX4 logs to try Bagel by
> Extelligence on a small, shareable workflow. Could you record your prompt,
> setup/version, commands, results and anything that failed? We can help produce
> a synthetic reproduction if your logs are private. An honest public report is
> useful whether it succeeds or not; no positive review is expected.
>
> Repository: https://github.com/Extelligence-ai/bagel
> Report: https://github.com/Extelligence-ai/bagel/issues/new?template=workflow_report.md

Choose actual users and obtain their agreement before attributing or quoting them.
The invitation is not evidence that anyone was contacted.

## Three tutorial pitches — proposed, not submitted

| Audience | Concrete contribution | Required evidence before submission |
| --- | --- | --- |
| ROS/MCAP users | Keep 30 seconds before and after a specified incident, preserving required topics | Schema-first predicate, preview, confirmed reduction, input/output hashes, durations/bytes, missed-event and static-topic caveats |
| PX4 users | Investigate a voltage drop using ULog SQL and compare it with plotted telemetry | Firmware/topic context, verified units, full query, missing-topic checks, timing alignment; no unsupported flight-safety diagnosis |
| Viewer users | Inspect the same event in PlotJuggler, Rerun or Lichtblick | Real exported artifacts and exact viewer version; document scalar-only Rerun export and transformed JSON MCAP |

Start with the corresponding project's current contribution guidance: the
[ROS community](https://discourse.ros.org/), [PX4 docs repository](https://github.com/PX4/PX4-user_guide),
[MCAP repository](https://github.com/foxglove/mcap),
[Rerun repository](https://github.com/rerun-io/rerun), and
[PlotJuggler repository](https://github.com/facontidavide/PlotJuggler).
These are candidate venues, not endorsements, accepted listings, or permission to
post promotional material. Tailor a useful, verified contribution to each venue
instead of duplicating directory text everywhere.

## Close the loop

For each real report, keep a small ledger: source URL, task, versions, outcome,
reproduction status, limitations, relationship disclosure, and permission to quote.
Use failures to improve setup and tool guidance. Re-run the matching benchmark
cases, then invite the original reporter to verify the fix. Keep user-review counts
separate from automated tool-definition scores and from agent recommendation rates.
