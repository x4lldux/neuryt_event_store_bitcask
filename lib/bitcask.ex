defmodule Bitcask do
  alias Bitcask.Database, as: DB

  defmodule Config do
    @moduledoc false
    def set_defaults() do
      defaults = [
        {:max_file_size, 2147483648},
        {:tombstone_version, 2},
        {:open_timeout, 4},
        {:sync_strategy, :none},
        {:require_hint_crc, false},
        {:merge_window, :always},
        {:frag_merge_trigger, 60},
        {:dead_bytes_merge_trigger, 536870912},
        {:frag_threshold, 40},
        {:dead_bytes_threshold, 134217728},
        {:small_file_threshold, 10485760},
        {:max_fold_age, -1},
        {:max_fold_puts, 0},
        {:expiry_secs, -1}
      ]
      for {key, val} <- defaults do
        if Application.get_env(:bitcask, key) == nil do
          Application.put_env(:bitcask, key, val)
        end
      end
    end
  end

  @doc ~S"""
  Opens up a Bitcask database in the given folder.

  To open up the database for writing into use options `[:read_write]`
  """
  @spec open(String.t, DB.options) :: DB.t
  def open(folder, options \\ []) do
    ref = :bitcask.open(to_char_list(folder), options)
    %DB{
      ref: ref,
      folder: folder,
      options: options
    }
  end

  @doc ~S"""
  Closes the access to a given database.
  """
  @spec close(DB.t) :: :ok
  def close(db), do: :bitcask.close(db.ref)

  @doc ~S"""
  Retreives a value by key.

  If the value was a serialized term it will be de-serialized before returning.
  """
  @spec get(DB.t, String.t) :: {:ok, binary()} | :not_found
  def get(db, key) when is_binary(key), do: decode(:bitcask.get(db.ref, key))

  @doc ~S"""
  Retreives a value by key. Throws if key is not found.

  If the value was a serialized term it will be de-serialized before returning.
  """
  @spec get!(DB.t, String.t) :: binary()
  def get!(db, key) do
    {:ok, val} = get(db, key)
    val
  end

  @doc ~S"""
  Puts a given value into the key.

  If a value is in binary or String format it will be put in as is.
  Other terms will be serialized into binary.
  """
  @spec put(DB.t, String.t, binary()) :: :ok
  def put(db, key, val) when is_binary(key), do: :bitcask.put(db.ref, key,
        encode(val))

  @doc ~S"""
  Delete the key. Bet you weren't expecting that.
  """
  @spec delete(DB.t, String.t) :: :ok
  def delete(db, key) when is_binary(key), do: :bitcask.delete(db.ref, key)

  @doc ~S"""
  Forces a sync of the database.
  """
  @spec sync(DB.t) :: :ok
  def sync(db), do: :bitcask.sync(db.ref)

  @doc ~S"""
  List all keys in the database.
  """
  @spec list_keys(DB.t) :: [binary()]
  def list_keys(db), do: :bitcask.list_keys(db.ref)

  @doc ~S"""
  Fold over the keys in the database.
  """
  @spec fold_keys(DB.t, function(), any) :: :ok
  def fold_keys(db, func, acc0), do: :bitcask.fold_keys(db.ref, func, acc0)

  # TODO: fold(Ref, Fun, Acc0, MaxAge, MaxPut, SeeTombstonesP)
  @doc ~S"""
  Fold over the keys and values.
  """
  @spec fold(DB.t, function(), List.t) :: :ok
  def fold(db, func, options \\ []) do
    :bitcask.fold(db.ref, &(func.(&1, encode(&2), &3)), options)
  end



  defp encode(val) when is_binary(val), do: val
  defp encode(val), do: "ETERM"<>:erlang.term_to_binary(val, [compressed: 3])

  defp decode({:ok, val}), do: {:ok, decode(val)}
  defp decode("ETERM"<>term), do: :erlang.binary_to_term(term)
  defp decode(val), do: val

end
