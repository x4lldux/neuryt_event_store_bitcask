defmodule Neuryt.EventStore.Bitcask do
  @behaviour Neuryt.EventStore

  def start_link() do
    Bitcask.Config.set_defaults
    db_path = Application.get_env :neuryt_event_store_bitcask, :db_path,
      "eventstore_db"
    db = Bitcask.open db_path, [:read_write]

    Agent.start_link(fn -> db end, name: __MODULE__)
    # Agent.start_link(&init/0, name: __MODULE__)
  end

  def stop() do
    Agent.get(__MODULE__, fn db -> Bitcask.close db end)
    Agent.stop __MODULE__
  end

  def init() do
    db_path = Application.get_env :neuryt_event_store_bitcask, :db_path,
      "eventstore_db"
    Bitcask.open db_path, [:read_write]
  end

  def save_event(event, stream_id) do
    save_events [event], stream_id
  end

  @event_ids_key "event_ids"
  def save_events(events, stream_id) do
    db = Agent.get(__MODULE__, &id/1)

    {:ok, cnt} = count_all_events
    events
    |> Enum.reduce(cnt, fn event, order_id ->
      event_key =
        {:event, event.id, stream_id, order_id}
        |> :erlang.term_to_binary
      :ok = Bitcask.put db, event_key, event

      order_id+1
    end)

    event_ids = events |> Enum.map(fn e -> e.id end)
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

    events =
      Bitcask.stream_keys(db)
      |> Stream.map(&:erlang.binary_to_term/1)
      |> Enum.filter(fn
        {:event, _event_id, _stream_id, _order_id} -> true
        _ -> false
      end)
      |> Enum.sort_by(& elem(&1, 3))
      |> Enum.map(fn event_key ->
        event_key = :erlang.term_to_binary event_key
        {:ok, e} = Bitcask.get(db, event_key)
        e
      end)

      {:ok, events}
  end

  def count_all_events() do
    db = Agent.get(__MODULE__, &id/1)

    count =
      Bitcask.stream_keys(db)
      |> Stream.map(&:erlang.binary_to_term/1)
      |> Enum.filter(fn
        {:event, _event_id, _stream_id, _order_id} -> true
        _ -> false
      end)
      |> length
    {:ok, count}
  end

  def load_stream_events(stream_id) do
    db = Agent.get(__MODULE__, &id/1)

    stream_key =
      {:stream, stream_id}
      |> :erlang.term_to_binary

    case Bitcask.get(db, stream_key) do
      {:ok, event_ids} ->
         events =
          event_ids
          |> Enum.map(fn id ->
               event_key =
                 id
                 |> get_event_key_for_id(db)
                 |> :erlang.term_to_binary
               {:ok, e} = Bitcask.get(db, event_key)
               e
             end)

        {:ok, Enum.reverse events}
      err ->
        err
    end
  end

  def count_stream_events(stream_id),
    do: {:ok, load_stream_events(stream_id) |> elem(1) |> length}

  def list_streams() do
    db = Agent.get(__MODULE__, &id/1)
    streams = Bitcask.fold_keys(db, &fold_stream_keys/2, [])
    {:ok, streams}
  end



  defp id(x), do: x

  defp get_event_key_for_id(event_id, db) do
    Bitcask.stream_keys(db)
    |> Stream.map(&:erlang.binary_to_term/1)
    |> Enum.find(nil, fn
      {:event, ^event_id, _stream_id, _order_id} -> true
      _ -> false
    end)
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
