# 日経先物 期近アイスバーン監視アプリ（CLI）

板情報（book）と歩み値（trade）を受け取り、
**大口注文の分割執行（アイスバーン注文）候補**をリアルタイムにアラートするPythonアプリです。

## 機能
- 板更新イベントから、同一価格の表示枚数リフィル（replenishment）を追跡
- 歩み値イベントから、同一価格への約定流入（absorbed volume）を追跡
- 「吸収量が大きい + リフィル回数が多い」価格をアイスバーン候補として通知
- しきい値をJSON設定で調整可能
- **SBI証券 HYPER SBI API(WebSocket) からのリアルタイム監視モード**を搭載

## セットアップ
```bash
python3 -m pip install websockets
```

## リアルタイム監視（HYPER SBI API）
```bash
python3 apps/nikkei_iceberg_monitor/monitor.py \
  --realtime \
  --ws-url "wss://<hyper-sbi-api-endpoint>" \
  --token "<access-token>" \
  --symbol "NK225" \
  --contract-month "202606" \
  --config apps/nikkei_iceberg_monitor/config.example.json
```

> `--symbol` / `--contract-month` で期近シンボルに絞り込めます。

### 受信メッセージの期待形式
HYPER SBI APIの配信を以下の形式として受け取る前提です（必要なら正規化層を拡張してください）。

#### 板（board）
```json
{"type":"board","ts":"2026-01-10T09:00:00","symbol":"NK225","contract_month":"202606","side":"ask","price":40320.0,"size":25}
```

#### 歩み値（execution）
```json
{"type":"execution","ts":"2026-01-10T09:00:01","symbol":"NK225","contract_month":"202606","aggressor":"buy","price":40320.0,"size":30}
```

## オフライン検証（JSONL）
```bash
python3 apps/nikkei_iceberg_monitor/monitor.py \
  --input apps/nikkei_iceberg_monitor/sample_feed.jsonl \
  --config apps/nikkei_iceberg_monitor/config.example.json
```

## アラート例
```text
[ALERT] iceberg候補: side=ask price=40320.0 absorbed=130 replenishments=2 visible=28
```

## 実運用メモ
- 受信payload形式が異なる場合は `hyper_sbi_client.py` の `normalize_hyper_sbi_payload` を調整
- Slack/LINE/メール通知は `print` をWebhook送信へ置換
- 本実装はヒューリスティック検知のため誤検知/見逃しがあり得ます
