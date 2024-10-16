defmodule EctoFoundationDB.Tenant.Managed do
  defstruct []

  @type t() :: %__MODULE__{}

  def new() do
    %__MODULE__{}
  end

  def list(db, start_name, end_name, options) do
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
          %ErlangError{original: {:erlfdb_error, 2133}} ->
            {:error, :tenant_nonempty}
        end
    end
  end

  def get(db, tenant_name) do
    case :erlfdb_tenant_management.get_tenant(db, tenant_name) do
      :not_found ->
        {:error, :tenant_does_not_exist}

      tenant ->
        {:ok, tenant}
    end
  end

  def open(db, tenant_name) do
    :erlfdb.open_tenant(db, tenant_name)
  end

  def all_data_ranges() do
    [{"", <<0xFF>>}]
  end

  def get_printable_name(json) do
    %{"name" => %{"printable" => name}} = Jason.decode!(json)
    name
  end

  def prepare_tuple(tuple, _meta) when is_tuple(tuple), do: tuple

  def prepare_tuple(function, meta) when is_function(function),
    do: function.(0) |> prepare_tuple(meta)

  def prepare_tuple(list, _meta) when is_list(list), do: :erlang.list_to_tuple(list)

  def recover_tuple(tuple, _meta), do: tuple
end
