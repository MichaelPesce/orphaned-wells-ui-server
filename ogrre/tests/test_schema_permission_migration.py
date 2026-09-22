"""CLI confirmation and target redaction never require a live database."""

from unittest.mock import Mock

import mongomock
import pytest

from ogrre import migrate_schema_permissions as migration


@pytest.fixture
def cli_db(monkeypatch):
    import dotenv
    from ogrre.internal import mongodb_connection

    db = mongomock.MongoClient().schema_migration_tests
    db.roles.insert_many(
        [
            {"id": "sys_admin", "category": "system", "permissions": ["manage_system"]},
            {"id": "team_lead", "category": "team", "permissions": ["review_record"]},
        ]
    )
    monkeypatch.setattr(dotenv, "load_dotenv", lambda: False)
    monkeypatch.setattr(mongodb_connection, "connectToDatabase", lambda: db)
    monkeypatch.setattr(mongodb_connection, "DB_NAME", db.name)
    monkeypatch.setattr(
        mongodb_connection,
        "DB_CONNECTION",
        "mongodb+srv://private-user:private-password@cluster.example.net/uri-database"
        "?authMechanismProperties=AWS_SESSION_TOKEN:private-token",
    )
    monkeypatch.setenv("COLLABORATOR", "test-collaborator")
    return db


@pytest.mark.parametrize(
    "connection, expected",
    [
        ("cluster.test", "mongodb+srv://cluster.test.mongodb.net"),
        ("mongodb://mongodb:27017", "mongodb://mongodb:27017"),
        (
            "mongodb://user:p%40ss@host1:27017,host2:27018/db?authSource=admin",
            "mongodb://host1:27017,host2:27018",
        ),
        ("mongodb://user:pass@[::1]:27017/db", "mongodb://[::1]:27017"),
        (None, "(not configured)"),
        ("mongodb://user:pass@[broken", "(unrecognized connection; details hidden)"),
    ],
)
def test_connection_label_omits_credentials_and_options(connection, expected):
    assert migration.connection_label(connection) == expected


def test_preview_identifies_target_without_writes_or_confirmation(
    cli_db, monkeypatch, capsys
):
    before = list(cli_db.roles.find())
    prompt = Mock(side_effect=AssertionError("Preview must not prompt"))
    monkeypatch.setattr("builtins.input", prompt)
    assert migration.main([]) == 0
    output = capsys.readouterr().out
    for expected in [
        "mongodb+srv://cluster.example.net",
        cli_db.name,
        "test-collaborator",
        "manage_schema_destructive",
        "Preview only",
    ]:
        assert expected in output
    for secret in ["private-user", "private-password", "private-token", "uri-database"]:
        assert secret not in output
    assert list(cli_db.roles.find()) == before
    assert cli_db.history.count_documents({}) == 0
    prompt.assert_not_called()


def test_apply_waits_for_approval_then_records_each_original_role(
    cli_db, monkeypatch, capsys
):
    before = list(cli_db.roles.find())

    def approve(prompt):
        output = capsys.readouterr().out
        assert cli_db.name in prompt
        assert "Proposed role changes:" in output
        assert "manage_schema_destructive" in output
        assert list(cli_db.roles.find()) == before
        assert cli_db.history.count_documents({}) == 0
        return "y"

    monkeypatch.setattr("builtins.input", approve)
    assert migration.main(["--apply"]) == 0
    assert "Applied changes to 2 role(s)." in capsys.readouterr().out
    assert migration.migrate_schema_permissions(cli_db) == []
    for role in before:
        history = cli_db.history.find_one({"query.role_id": role["id"]})
        assert history["previous_state"]["permissions"] == role["permissions"]


@pytest.mark.parametrize("answer", ["", "n", "yes", EOFError(), KeyboardInterrupt()])
def test_apply_cancellation_never_writes(cli_db, monkeypatch, capsys, answer):
    before = list(cli_db.roles.find())
    prompt = (
        Mock(side_effect=answer)
        if isinstance(answer, BaseException)
        else Mock(return_value=answer)
    )
    monkeypatch.setattr("builtins.input", prompt)
    assert migration.main(["--apply"]) == 1
    assert "Cancelled. No changes were applied." in capsys.readouterr().out
    assert list(cli_db.roles.find()) == before
    assert cli_db.history.count_documents({}) == 0


@pytest.mark.parametrize("new_role", [False, True])
def test_apply_rejects_changes_during_confirmation(
    cli_db, monkeypatch, capsys, new_role
):
    def approve(prompt):
        if new_role:
            cli_db.roles.insert_one(
                {"id": "developer", "category": "system", "permissions": []}
            )
        else:
            cli_db.roles.update_one(
                {"id": "team_lead"}, {"$push": {"permissions": "manage_team"}}
            )
        return "y"

    monkeypatch.setattr("builtins.input", approve)
    assert migration.main(["--apply"]) == 1
    assert "differ from the approved preview" in capsys.readouterr().out
    assert cli_db.roles.count_documents({"permissions": "manage_schema"}) == 0
    assert cli_db.history.count_documents({}) == 0


def test_apply_does_not_prompt_when_no_changes_remain(cli_db, monkeypatch, capsys):
    migration.migrate_schema_permissions(cli_db, apply=True)
    prompt = Mock(side_effect=AssertionError("No changes must not prompt"))
    monkeypatch.setattr("builtins.input", prompt)
    assert migration.main(["--apply"]) == 0
    assert "No changes needed." in capsys.readouterr().out
    assert cli_db.history.count_documents({}) == 2
    prompt.assert_not_called()
