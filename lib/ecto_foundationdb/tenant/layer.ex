defmodule EctoFOundationDB.Tenant.Layer do
  defstruct [:prefix]

  @type t() :: %__MODULE__{}

  def new(prefix) do
    %__MODULE__{prefix: prefix}
  end

  def list(db, start_name, end_name, options) do
  end

  def create(db, tenant_name, options) do
  end

  def delete(db, tenant_name, options) do
  end

  def get(db, tenant_name) do
  end

  def open(db, tenant_name) do
  end

  def all_data_ranges() do
  end

  def get_printable_name(db_object) do
  end

  def prepare_tuple(x, meta), do: add_tuple_head(x, meta.prefix)

  def recover_tuple(tuple, _meta), do: delete_tuple_head(tuple)

  defp add_tuple_head(tuple, head) when is_tuple(tuple) do
    :erlang.insert_element(1, tuple, head)
  end

  defp add_tuple_head(list, head) when is_list(list) do
    [head | list]
    |> :erlang.list_to_tuple()
  end

  defp add_tuple_head(function, head) when is_function(function) do
    function.(1)
    |> add_tuple_head(head)
  end

  defp delete_tuple_head(tuple) do
    :erlang.delete_element(1, tuple)
  end
end
