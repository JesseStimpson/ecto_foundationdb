defmodule Ecto.Adapters.FoundationDB.Record.Pack do
  def to_fdb_key(adapter_opts, source, pk) do
    # TODO: support non-binary pks
    Enum.join([source, pk], adapter_opts[:key_delimiter])
  end

  def to_fdb_key_startswith(adapter_opts, source) do
    Enum.join([source, ""], adapter_opts[:key_delimiter])
  end

  def to_fdb_value(fields), do: :erlang.term_to_binary(fields)

  def from_fdb_value(bin), do: :erlang.binary_to_term(bin)
end