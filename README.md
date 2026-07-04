# ai-store

AI 向け Store Backend. Domain 非依存の append-only event store + revert-as-commit + FileProjection を提供する共有 crate 群。

## Workspace

| crate | 役割 |
|---|---|
| `ai-store-core` | facade + SPI trait + 型。infra 依存ゼロ (json-patch / serde / thiserror / uuid / time のみ) |
| `ai-store-sqlite` | EventBackend + CacheBackend の rusqlite 実装 (rusqlite-isle で並行性 offload) |
| `ai-store-mem` | in-memory backend (test / 軽量用途) |
| `ai-store-fileproj` | ProjectionSink の file 実装 (draft.md / label.md 書き出し) |

## 設計核

- **append-only を型で構造保証**: EventBackend は delete / overwrite に相当する method を持たない
- **diff-based history**: history の SoT は full snapshot ではなく JSON Patch (RFC 6902) の event log。snapshot は derived cache に降格
- **revert-as-commit**: 復元は逆差分の単一 event append (1 tx)。原子性は SQLite tx で構造保証
- **単一書き込み口**: consumer は `Store` facade のみ触る。SchemaGate を通らない append 経路は API として存在しない
- **async facade / sync backend**: consumer (tokio async MCP) は async facade だけ見る。sync backend (rusqlite) は writer actor で内部封じ込め

## 状態

MVP 実装中 (`ai-store-core` skeleton land 済)。

## License

TBD
