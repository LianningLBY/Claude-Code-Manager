# 数据库管理指南

本项目使用 **Alembic** 管理 SQLite schema 版本。所有 schema 变更必须通过 Alembic migration 完成，不允许手动修改数据库文件或 `ALTER TABLE`。

---

## 日常使用

### 启动应用

`init_db()` 在应用启动时自动调用 `alembic upgrade head`，无需手动干预。

### 手动升级（拉取别人的新 migration 后）

```bash
uv run alembic upgrade head
```

### 查看当前版本

```bash
uv run alembic current
```

### 查看历史

```bash
uv run alembic history --verbose
```

---

## 写新 Migration

### 1. 修改 SQLAlchemy 模型

在 `backend/models/` 下修改对应模型文件，添加、删除或修改字段。

### 2. 自动生成 migration 文件

```bash
uv run alembic revision --autogenerate -m "描述变更内容"
```

Alembic 会对比当前模型和数据库 schema 差异，生成 `alembic/versions/` 下的新文件。

> **注意**：自动生成的内容需要人工检查。Alembic 无法检测：列改名（会生成 drop+add）、约束变更的语义等。生成后务必打开文件核查。

### 3. 测试 migration（提交前必做）

```bash
# 升级到最新
uv run alembic upgrade head

# 验证降级可以回滚（确保 downgrade 逻辑正确）
uv run alembic downgrade -1

# 再升级回来，确认数据完整
uv run alembic upgrade head
```

只有 upgrade + downgrade + upgrade 三步全部通过，才可以将 migration 文件提交到 git。

### 4. 提交到 git

```bash
git add alembic/versions/<新文件>.py backend/models/<修改的模型>.py
git commit -m "feat: 添加 XXX 字段"
```

**migration 文件必须和模型修改在同一个 commit 中提交。**

---

## 多端协同

多人同时开发、各自拉取代码时，按以下步骤确保数据库同步：

```bash
git pull
uv run alembic upgrade head   # 应用别人新增的 migration
```

### 处理 migration 冲突

当两个分支各自新增了 migration（`down_revision` 相同），合并后会出现分叉（branch）。解决步骤：

```bash
uv run alembic heads           # 查看当前所有 head
uv run alembic history         # 查看分叉点
```

手动编辑其中一个 migration 文件，将 `down_revision` 改为另一个 migration 的 revision ID，形成线性链，然后：

```bash
uv run alembic upgrade head    # 验证合并后的链路正常
```

解决完冲突后按正常流程测试并提交。

---

## 回滚

```bash
# 回滚最近一个 migration
uv run alembic downgrade -1

# 回滚到指定版本
uv run alembic downgrade 6b3f8a1c2d9e
```

---

## 首次拉取本项目（已有旧数据库的用户）

如果你在 Alembic 引入之前就已有数据库（schema 是通过手动 `ALTER TABLE` 应用的），应用启动时会自动检测并执行 `alembic stamp head`，将数据库标记为最新版本。无需手动操作。

如果自动检测失败，手动执行：

```bash
uv run alembic stamp head
```

---

## Migration 文件规范

- **文件位置**：`alembic/versions/`
- **命名格式**：`<revision_id>_<描述>.py`（由 Alembic 自动生成）
- **已发布 revision 不可删除或改写**：曾进入可部署分支的 migration 必须永久保留。回滚功能时新增一个以旧 revision 为 `down_revision` 的前向清理 migration，禁止删除旧文件、改 revision ID 或手动 `stamp` 绕过历史。
- **必须包含 downgrade**：`downgrade()` 函数必须实现，能够回滚所有 `upgrade()` 的变更
- **SQLite 的 ALTER 限制**：修改已有列（改类型、改 nullable）必须使用 batch 模式：

  ```python
  with op.batch_alter_table('table_name') as batch_op:
      batch_op.alter_column('col', existing_type=sa.Text(), nullable=True)
  ```

- **新模型要在 `alembic/env.py` 中 import**：确保 autogenerate 能检测到新表

---

## 现有 Migration 记录

| Revision ID      | 描述                              | 日期       |
|------------------|-----------------------------------|------------|
| `6b3f8a1c2d9e`   | Initial schema（所有初始表）       | 2025-01-01 |
| `c4d7e2f9a0b1`   | Loop task 字段（todo_file_path 等）| 2026-03-07 |
| `b6e1f4a2c9d7`   | Independent Plan schema（历史 revision，永久保留） | 2026-07-29 |
| `f7a1c3d9e5b2`   | 清理已撤回的 Independent Plan schema | 2026-07-31 |
