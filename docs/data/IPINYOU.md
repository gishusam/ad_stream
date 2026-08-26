# iPinYou data for AdStream

AdStream's primary demonstration source is the public iPinYou real-time bidding dataset. The dataset contains real advertising campaign logs including impression opportunities, advertiser and creative identifiers, bids, paying prices, clicks and related auction context.

## Why the repository does not download or commit the dataset

The raw dataset is large and its distribution terms require users to obtain it from the dataset publisher/research mirrors. AdStream therefore keeps dataset acquisition manual and ignores `data/external/` in Git.

References:

- Dataset description: https://contest.ipinyou.com/ipinyou-dataset.pdf
- Dataset formatter: https://github.com/wnzhang/make-ipinyou-data
- The formatter README links to the UCL-hosted `ipinyou.contest.dataset.zip`.

## Expected input

This first integration consumes the formatted `train.log.txt` or `test.log.txt` created by `make-ipinyou-data`. For example:

```text
data/external/ipinyou/1458/train.log.txt
```

The formatted file has the `click`, `weekday`, and `hour` columns followed by the published iPinYou schema. AdStream keeps `bidprice` and `payprice` in the source-native RMB/CPM units rather than pretending they are USD revenue.

## Replay into Kafka

Start the existing local Kafka stack:

```bash
make up
```

Replay a small real slice first:

```bash
python -m src.producers.ipinyou_replay_producer \
  data/external/ipinyou/1458/train.log.txt \
  --limit 1000 \
  --events-per-second 100
```

The events are written to the existing `ad.impressions.raw` topic and keyed by advertiser ID.

## Bronze smoke test

In another terminal, run the existing Bronze consumer:

```bash
python -m src.processing.bronze_ingestion
```

The canonical event preserves the source bid ID, bid price, paying price, click outcome, ad exchange and slot ID. `is_fraud` is explicitly `false` for imported iPinYou events; the ingestion layer does not fabricate a fraud label.
