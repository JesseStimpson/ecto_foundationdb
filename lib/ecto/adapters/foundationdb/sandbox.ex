defmodule Ecto.Adapters.FoundationDB.Sandbox do
  @moduledoc """
  A module for managing a sandbox FoundationDB cluster. This allows a developer to create
  a space under which it should be safe to write tests.

  When using this module, it creates a directory in the project root called `.erlfdb`. It
  is safe to delete this directory when you no longer need it (e.g. after test execution)
  """
  alias Ecto.Adapters.FoundationDB.Tenant

  def open_db() do
    get_or_create_test_db()
  end

  def checkout(repo, id, migration) when is_binary(id) do
    case :persistent_term.get({__MODULE__, id, :tenant}, nil) do
      nil ->
        db = get_or_create_test_db()
        tenant = Tenant.open_empty!(db, id, repo.config())

        :ok = Ecto.Migrator.up(repo, 0, migration, prefix: id, log: false)

        :persistent_term.put({__MODULE__, id, :tenant}, tenant)
        tenant

      _ ->
        raise "FoundationDB Sandbox Tenant named #{repo} is already checked out"
    end
  end

  def checkin(repo, id) when is_binary(id) do
    db = :persistent_term.get({__MODULE__, :database})
    Tenant.clear_delete!(db, id, repo.config())
    :persistent_term.erase({__MODULE__, id, :tenant})
  end

  defp get_or_create_test_db() do
    case :persistent_term.get({__MODULE__, :database}, nil) do
      nil ->
        new_db = :erlfdb_util.get_test_db([])
        :persistent_term.put({__MODULE__, :database}, new_db)
        new_db

      already_initted_db ->
        already_initted_db
    end
  end
end
