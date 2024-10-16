defmodule Ecto.Adapters.FoundationDB.EctoAdapterStorage do
  @moduledoc false
  @behaviour Ecto.Adapter.Storage

  alias EctoFoundationDB.Options
  alias EctoFoundationDB.Tenant

  @storage_id ""
  @storage_delimiter_override ""

  def open_db(options) do
    fun = Options.get(options, :open_db)
    fun.()
  end

  @impl true
  def storage_up(options) do
    db = open_db(options)

    case Tenant.get(db, @storage_id, storage_options(options)) do
      {:error, :tenant_does_not_exist} ->
        :ok = Tenant.create(db, @storage_id, storage_options(options))
        :ok

      {:ok, _} ->
        {:error, :already_up}
    end
  end

  @impl true
  def storage_down(options) do
    db = open_db(options)

    case Tenant.get(db, @storage_id, storage_options(options)) do
      {:error, :tenant_does_not_exist} ->
        {:error, :already_down}

      {:ok, _} ->
        Tenant.clear(db, @storage_id, storage_options(options))
        :ok = Tenant.delete(db, @storage_id, storage_options(options))
        :ok
    end
  end

  @impl true
  def storage_status(options) do
    db = open_db(options)

    case Tenant.get(db, @storage_id, storage_options(options)) do
      {:ok, _} ->
        :up

      _ ->
        :down
    end
  end

  def was_up_with_managed_tenants?(db, options) do
    case managed_tenants_status(db, options) do
      {:ok, _} ->
        true

      _ ->
        false
    end
  end

  def was_up_with_layer_tenants?(_db, _options) do
    # @todo
    false
  end

  defp managed_tenants_status(db, options) do
    try do
      Tenant.get(db, @storage_id, storage_options(options))
    rescue
      e in ErlangError ->
        case e do
          %ErlangError{original: {:erlfdb_error, 2136}} ->
            {:error, :tenants_disabled}
        end
    end
  end

  def open_storage_tenant(db, options) do
    Tenant.open(db, storage_options(options))
  end

  defp storage_options(options), do: Keyword.merge(options, storage_options_override())

  defp storage_options_override(), do: [storage_delimiter: @storage_delimiter_override]
end
