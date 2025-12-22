# Marketing Campaign Engagement Analysis
**Question:** When do our campaigns engage users most, and how does engagement differ by weekday, channel, and campaign?

## Business context
As a Marketing Analyst, I was asked to identify weekday-level engagement patterns across marketing campaigns and channels and to share insights that could inform timing and landing-page improvements.

## Data
- Source: `project-477112.raw_events` (event-level website tracking)
- Constraints: no session identifiers → sessions were **modeled** using a **30-minute inactivity threshold**

## Method 
- **Sessionization:** for each user, a new session starts if the time since the previous event exceeds 30 minutes.
- **Session duration:** calculated as the difference between the first and last event timestamp within a session.
- **Attribution:** each session was assigned a landing campaign using the first non-null campaign value observed in the session.
- **Aggregation:** average session duration was analyzed by weekday and by campaign group (Paid, Organic, Direct, Referral).
- **Reliability filtering:** campaign-level analysis excluded with very small session counts to reduce noise.

## Key findings
- Paid marketing campaigns drive the longest average session duration, peaking mid-week at approximately **6.1 minutes**.
- The mid-week engagement pattern is visible at both channel and campaign levels, although campaign-level results are limited by small sample sizes.
- Most users who convert do so within their **first two sessions**, with conversion likelihood dropping sharply afterward.
- Roughly **one in three purchases occurs during sessions lasting 6–15 minutes**, suggesting this window reflects the highest conversion intent.

## Recommendations
- **Lean into mid-week momentum:** test concentrating paid campaigns, emails, or product highlights on Wednesdays and Thursdays.
- **Improve attribution quality:** audit and fix tracking gaps, particularly sessions labeled as unattributed paid traffic.
- **Optimize landing pages:** review organic and direct landing experiences to improve early-session engagement.

## Limitations
- A large portion of paid traffic is labeled as unattributed due to broken tracking.
- Session duration is a proxy for engagement and does not fully explain user intent.
- Session modeling introduces minor discrepancies compared to raw event-level counts.

## Deliverables
- **Slides:** Executive presentation with visual analysis [slides](slides/)
- **SQL:** Queries used for session modeling and analysis [sql](sql/)
