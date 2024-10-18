defmodule EctoFoundationDB.Tenant do
  @moduledoc """
  This module allows the application to manage the creation and deletion of
  tenants within the FoundationDB database. All transactions require a tenant,
  so any application that uses the Ecto FoundationDB Adapter must use this module.
  """

  defstruct [:txobj, :meta]

  alias Ecto.Adapters.FoundationDB, as: FDB
  alias Ecto.Adapters.FoundationDB.EctoAdapterStorage

  alias EctoFoundationDB.Database
  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Migrator
  alias EctoFoundationDB.Options
  alias EctoFoundationDB.Tenant
  alias EctoFoundationDB.Tenant.Managed
  alias EctoFoundationDB.Tenant.Layer

  @type t() :: %Tenant{}
  @type meta() :: Managed.t() | Layer.t()
  @type id() :: :erlfdb.tenant_name()
  @type prefix() :: String.t()

  def txobj(%Tenant{txobj: txobj}), do: txobj

  @doc """
  Returns true if the tenant already exists in the database.
  """
  @spec exists?(Ecto.Repo.t(), id()) :: boolean()
  def exists?(repo, id), do: exists?(FDB.db(repo), id, repo.config())

  @doc """
  Create a tenant in the database.
  """
  @spec create(Ecto.Repo.t(), id()) :: :ok
  def create(repo, id), do: create(FDB.db(repo), id, repo.config())

  @doc """
  Clears data in a tenant and then deletes it. If the tenant doesn't exist, no-op.
  """
  @spec clear_delete!(Ecto.Repo.t(), id()) :: :ok
  def clear_delete!(repo, id) do
    options = repo.config()
    db = FDB.db(repo)

    if exists?(db, id, options) do
      :ok = clear(db, id, options)
      :ok = delete(db, id, options)
    end

    :ok
  end

  @doc """
  Open a tenant with a repo. With the result returned by this function, the caller can
  do database operations on the tenant's portion of the key-value store.

  The tenant must already exist.

  When opening tenants with a repo, all migrations are automatically performed. This
  can cause open/2 to take a significant amount of time. Tenants can be kept open
  indefinitely, with any number of database transactions issued upon them.
  """
  @spec open(Ecto.Repo.t(), id(), Options.t()) :: t()
  def open(repo, id, options \\ []) do
    config = Keyword.merge(repo.config(), options)
    tenant = db_open(FDB.db(repo), id, config)
    handle_open(repo, tenant, config)
    tenant
  end

  @doc """
  Open a tenant. With the result returned by this function, the caller can
  do database operations on the tenant's portion of the key-value store.

  If the tenant does not exist, it is created.

  When opening tenants with a repo, all migrations are automatically performed. This
  can cause open/2 to take a significant amount of time. Tenants can be kept open
  indefinitely, with any number of database transactions issued upon them.
  """
  @spec open!(Ecto.Repo.t(), id(), Options.t()) :: t()
  def open!(repo, id, options \\ []) do
    config = Keyword.merge(repo.config(), options)
    tenant = db_open!(FDB.db(repo), id, config)
    handle_open(repo, tenant, config)
    tenant
  end

  @doc """
  Helper function to ensure the given tenant exists and then clear
  it of all data, and finally return an open handle. Useful in test code,
  but in production, this would be dangerous.
  """
  @spec open_empty!(Ecto.Repo.t(), id(), Options.t()) :: t()
  def open_empty!(repo, id, options_in \\ []) do
    db = FDB.db(repo)
    options = Keyword.merge(repo.config(), options_in)
    :ok = ensure_created(db, id, options)
    :ok = empty(db, id, options)
    open(repo, id, options_in)
  end

  @doc """
  List all tenants in the database. Could be expensive.
  """
  @spec list(Ecto.Repo.t()) :: [id()]
  def list(repo), do: list(FDB.db(repo), repo.config())

  @doc """
  Clear all data for the given tenant. This cannot be undone.
  """
  @spec clear(Ecto.Repo.t(), id()) :: :ok
  def clear(repo, id), do: clear(FDB.db(repo), id, repo.config())

  @doc """
  Deletes a tenant from the database permanently. The tenant must
  have no data.
  """
  @spec delete(Ecto.Repo.t(), id()) :: :ok
  def delete(repo, id), do: delete(FDB.db(repo), id, repo.config())

  @spec db_open!(Database.t(), id(), Options.t()) :: t()
  def db_open!(db, id, options) do
    :ok = ensure_created(db, id, options)
    db_open(db, id, options)
  end

  @doc """
  If the tenant doesn't exist, create it. Otherwise, no-op.
  """
  @spec ensure_created(Database.t(), id(), Options.t()) :: :ok
  def ensure_created(db, id, options) do
    case exists?(db, id, options) do
      true -> :ok
      false -> create(db, id, options)
    end
  end

  @doc """
  Returns true if the tenant exists in the database. False otherwise.
  """
  @spec exists?(Database.t(), id(), Options.t()) :: boolean()
  def exists?(db, id, options) do
    case get(db, id, options) do
      {:ok, _} -> true
      {:error, :tenant_does_not_exist} -> false
    end
  end

  @spec db_open(Database.t(), id(), Options.t()) :: t()
  def db_open(db, id, options) do
    module = get_module(options)
    tenant_name = module.get_name(id, options)
    opened = module.open(db, tenant_name, options)
    meta = module.make_meta(opened)

    %Tenant{txobj: module.txobj(db, opened, meta), meta: meta}
  end

  @spec list(Database.t(), Options.t()) :: [id()]
  def list(db, options) do
    module = get_module(options)
    list = module.list(db, [])

    for {_k, db_object} <- list do
      module.get_id(db_object, options)
    end
  end

  @spec create(Database.t(), id(), Options.t()) :: :ok
  def create(db, id, options) do
    module = get_module(options)
    tenant_name = module.get_name(id, options)

    module.create(db, tenant_name, options)
  end

  @spec clear(Database.t(), id(), Options.t()) :: :ok
  def clear(db, id, options) do
    tenant = db_open(db, id, options)

    ranges =
      get_module(options).all_data_ranges(tenant.meta)

    :erlfdb.transactional(txobj(tenant), fn tx ->
      for {start_key, end_key} <- ranges, do: :erlfdb.clear_range(tx, start_key, end_key)
    end)

    :ok
  end

  @spec empty(Database.t(), id(), Options.t()) :: :ok
  def empty(db, id, options) do
    tenant = db_open(db, id, options)

    {start_key, end_key} =
      Pack.adapter_repo_range(tenant)

    :erlfdb.transactional(txobj(tenant), fn tx ->
      :erlfdb.clear_range(tx, start_key, end_key)
    end)

    :ok
  end

  @spec delete(Database.t(), id(), Options.t()) :: :ok | {:error, atom()}
  def delete(db, id, options) do
    module = get_module(options)
    tenant_name = module.get_name(id, options)

    module.delete(db, tenant_name, options)
  end

  def pack(tenant, tuple) when is_tuple(tuple) do
    tuple
    |> tenant.meta.__struct__.extend_tuple(tenant.meta)
    |> :erlfdb_tuple.pack()
  end

  def unpack(tenant, tuple) do
    tuple
    |> :erlfdb_tuple.unpack()
    |> tenant.meta.__struct__.extract_tuple(tenant.meta)
  end

  def range(tenant, tuple) when is_tuple(tuple) do
    tuple
    |> tenant.meta.__struct__.extend_tuple(tenant.meta)
    |> :erlfdb_tuple.range()
  end

  def primary_mapper(tenant) do
    # mapper indexes are offset by the number of elements added by `extend_tuple`
    fn offset ->
      # tuple elements: (head,) prefix, source, namespace, id, get_range
      for(i <- offset..(offset + 3), do: "{V[#{i}]}") ++ ["{...}"]
    end
    |> tenant.meta.__struct__.extend_tuple(tenant.meta)
  end

  defp handle_open(repo, tenant, options) do
    Migrator.up(repo, tenant, options)
  end

  def get(db, id, options) do
    module = get_module(options)
    tenant_name = module.get_name(id, options)
    module.get(db, tenant_name, options)
  end

  defp get_module(options) do
    case Options.get(options, :tenant_type) do
      :managed ->
        Managed

      :layer ->
        Layer
    end
  end

  def assert_safe!(db, options) do
    module = get_module(options)

    if module == Layer and EctoAdapterStorage.was_up_with_managed_tenants?(db, options) do
      raise Unsupported, """
      Your FoundationDB database was previously created with `tenant_type: :managed`. The default
      changed to `tenant_type: :layer` as of version 0.3, and it's not safe for us to start
      EctoFoundationDB with `tenant_type: :layer` when it had previously been `:managed`. Please
      change `tenant_type: :managed` or start with an empty database.
      """
    end

    if module == Managed and EctoAdapterStorage.was_up_with_layer_tenants?(db, options) do
      raise Unsupported, """
      Your FoundationDB database was previously created with `tenant_type: :layer`, and you're now
      requesting `tenant_type: :managed`. Changing the tenant_type is not supported. Please change
      `tenant_type: :layer` or start with an empty database.
      """
    end

    :ok
  end
end
