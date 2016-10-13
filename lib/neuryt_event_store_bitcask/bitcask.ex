defmodule Neuryt.EventStore.Bitcask do
  @behaviour Neuryt.EventStore

  def start_link() do
    db_path = Application.get_env :neuryt_event_store_bitcask, :db_path, "eventstore_db"
    db = Bitcask.open db_path, [:read_write]

    Agent.start_link(fn -> db end, name: __MODULE__)
    # Agent.start_link(&init/0, name: __MODULE__)
  end

  def stop() do
    Agent.get(__MODULE__, fn db -> Bitcask.close db end)
    Agent.stop __MODULE__
  end

  def init() do
    db_path = Application.get_env :neuryt_event_store_bitcask, :db_path, "eventstore_db"
    Bitcask.open db_path, [:read_write]
  end

  def save_event(event, stream_id) do
    save_events [event], stream_id
  end

  def save_events(events, stream_id) do
    db = Agent.get(__MODULE__, &id/1)

    events
    |> Enum.each(fn event ->
      event_key =
        {:event, event.id, stream_id}
        |> :erlang.term_to_binary

      :ok = Bitcask.put db, event_key, event
    end)

    event_ids =
      events
      |> Enum.map(fn e -> e.id end)

    stream_key =
      {:stream, stream_id}
      |> :erlang.term_to_binary
    stream = case Bitcask.get(db, stream_key) do
               {:ok, stream} -> stream
               :not_found -> []
             end
    :ok = Bitcask.put db, stream_key, event_ids ++ stream
    Bitcask.sync db

    :ok
  end

  def load_all_events() do
    db = Agent.get(__MODULE__, &id/1)
    events = Bitcask.fold_keys(db, &fold_event_keys/2, [])

    {:ok, events}
  end

  def count_all_events(),
    do: {:ok, load_all_events() |> elem(1) |> length}

  def load_stream_events(stream_id) do
    db = Agent.get(__MODULE__, &id/1)

    stream_key =
      {:stream, stream_id}
      |> :erlang.term_to_binary
    events = Bitcask.get(db, stream_key)
    {:ok, events}
  end

  def count_stream_events(stream_id),
    do: {:ok, load_stream_events(stream_id) |> elem(1) |> length}

  def list_streams() do
    db = Agent.get(__MODULE__, &id/1)
    streams = Bitcask.fold_keys(db, &fold_stream_keys/2, [])
    {:ok, streams}
  end



  defp id(x), do: x

  def fold_event_keys(key_entry, acc) do
    {:bitcask_entry, key_blob, _, _, _, _} = key_entry
    key = :erlang.binary_to_term(key_blob)
    case key do
      {:event, event_id, _} -> [event_id | acc]
      _ -> acc
    end
  end

  def fold_stream_keys(key_entry, acc) do
    {:bitcask_entry, key_blob, _, _, _, _} = key_entry
    key = :erlang.binary_to_term(key_blob)
    case key do
      {:stream, stream_id} -> [stream_id | acc]
      _ -> acc
    end
  end
end
