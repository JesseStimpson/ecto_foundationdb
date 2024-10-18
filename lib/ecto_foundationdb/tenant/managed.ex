defmodule EctoFoundationDB.Tenant.Managed do
  defstruct []

  alias EctoFoundationDB.Options

  @type t() :: %__MODULE__{}

  def txobj(_db, tenant_ref) do
    tenant_ref
  end

  def make_meta(_tenant_ref) do
    %__MODULE__{}
  end

  def get_name(id, options) do
    storage_id = Options.get(options, :storage_id)
    storage_delimiter = Options.get(options, :storage_delimiter)

    "#{storage_id}#{storage_delimiter}#{id}"
  end

  def list(db, options) do
    start_name = get_name("", options)
    end_name = :erlfdb_key.strinc(start_name)
    :erlfdb_tenant_management.list_tenants(db, start_name, end_name, options)
  end

  def create(db, tenant_name, _options) do
    try do
      :erlfdb_tenant_management.create_tenant(db, tenant_name)
    rescue
      e in ErlangError ->
        case e do
          %ErlangError{original: {:erlfdb_error, 2132}} ->
            {:error, :tenant_already_exists}
        end
    end
  end

  def delete(db, tenant_name, _options) do
    try do
      :erlfdb_tenant_management.delete_tenant(db, tenant_name)
    rescue
      e in ErlangError ->
        case e do
          %ErlangError{
            original: {:erlfdb_directory, {:remove_error, :path_missing, [utf8: ^tenant_name]}}
          } ->
            {:error, :tenant_nonempty}
        end
    end
  end

  def get(db, tenant_name, _options) do
    case :erlfdb_tenant_management.get_tenant(db, tenant_name) do
      :not_found ->
        {:error, :tenant_does_not_exist}

      tenant ->
        {:ok, tenant}
    end
  end

  def open(db, tenant_name, _options) do
    :erlfdb.open_tenant(db, tenant_name)
  end

  def all_data_ranges(_meta) do
    [{"", <<0xFF>>}]
  end

  def get_id(json, options) do
    %{"name" => %{"printable" => name}} = Jason.decode!(json)
    tenant_name_to_id!(name, options)
  end

  def extend_tuple(tuple, _meta) when is_tuple(tuple), do: tuple

  def extend_tuple(function, meta) when is_function(function),
    do: function.(0) |> extend_tuple(meta)

  def extend_tuple(list, _meta) when is_list(list), do: :erlang.list_to_tuple(list)

  def extract_tuple(tuple, _meta), do: tuple

  defp tenant_name_to_id!(tenant_name, options) do
    prefix = get_name("", options)
    len = String.length(prefix)
    ^prefix = String.slice(tenant_name, 0, len)
    String.slice(tenant_name, len, String.length(tenant_name) - len)
  end
end
