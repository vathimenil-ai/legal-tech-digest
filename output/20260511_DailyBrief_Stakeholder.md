# Legal Tech Intelligence — Sunday, May 11, 2026

## Bottom Line

- **Anthropic's inference infrastructure is spreading across three cloud providers, and most legal AI buyers haven't noticed.** Akamai's reported $1.8B, seven-year compute deal with Anthropic means Claude inference now runs on AWS, Google Cloud, and Akamai. For any legal services provider whose AI products depend on Claude-based vendors — Harvey being the most prominent — the question "where does my data actually get processed?" just got harder to answer, especially for government-facing work.

## What Changed

### Anthropic's multi-cloud inference creates a new procurement problem for legal AI buyers

**What happened:** Bloomberg and The Information report that Akamai has secured a $1.8 billion, seven-year compute commitment from Anthropic, distributing Claude's inference workload across a third major cloud provider alongside AWS and Google Cloud.

**What's new:** Until now, the upstream AI model provider risk for legal AI buyers was largely theoretical — a concern about Anthropic's government supply chain designation and what it might mean for vendor qualification. This deal makes the problem concrete and structural. Anthropic isn't consolidating its infrastructure; it's deliberately diversifying it. That means a legal AI vendor running on Claude can't easily tell a client which cloud is processing their data on any given inference call. For routine commercial work, this may not matter much. For government-adjacent legal work subject to FedRAMP, ITAR, or data sovereignty rules, it matters a great deal.

**Why it matters:** Legal services providers that sell AI-powered workflows to government clients or regulated industries now face a second layer of vendor qualification they didn't have before. It's not enough to vet your direct AI vendor's security posture — you need to understand the cloud infrastructure beneath it, and that infrastructure is becoming more complex, not less. This is precisely the kind of cross-layer governance question that point-solution AI tools aren't built to answer. Legal services firms with deep procurement and compliance operations are better positioned to help clients navigate this than the AI vendors themselves — turning upstream supply chain opacity into a governance service line rather than just a risk factor. The practical question is whether Claude-dependent legal AI vendors like Harvey will address their infrastructure governance publicly, or whether buyers will have to push for answers. [Sources: 1]

## Watch Next

- **Whether Harvey or other Claude-dependent legal AI vendors issue public statements about their multi-cloud inference governance** — particularly regarding data residency and security certification for government-facing work. Silence here would be telling: it would suggest the downstream legal AI ecosystem hasn't confronted the supply chain question that Anthropic's infrastructure strategy is creating.

---

## Sources

1. Akamai-Anthropic deal implications for legal AI vendor risk — ComplexDiscovery, May 9, 2026 — https://complexdiscovery.com/what-akamais-reported-anthropic-deal-means-for-legal-ai-vendor-risk/