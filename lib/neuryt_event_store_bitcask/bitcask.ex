defmodule Neuryt.EventStore.Bitcask do
  @behaviour Neuryt.EventStore
  use GenServer

  #################################
  # Client API

  def start_link() do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def stop() do
    GenServer.stop __MODULE__
  end

  def save_event(event, stream_id) do
    save_events [event], stream_id
  end

  def save_events(events, stream_id) do
    GenServer.call __MODULE__, {:save_events, events, stream_id}
  end

  def load_all_events() do
    GenServer.call __MODULE__, :load_all_events
  end

  def count_all_events() do
    GenServer.call __MODULE__, :count_all_events
  end

  def load_stream_events(stream_id) do
    GenServer.call __MODULE__, {:load_stream_events, stream_id}
  end

  def count_stream_events(stream_id) do
    case load_stream_events(stream_id) do
      {:ok, events} -> {:ok, length events}
      {:error, reason} -> {:error, reason}
    end
  end

  def list_streams() do
    GenServer.call __MODULE__, :list_streams
  end

  #################################
  # GenServer callback

  def init(:ok) do
    Bitcask.Config.set_defaults
    db_path = Application.get_env :neuryt_event_store_bitcask, :db_path,
      "eventstore_db"
    db = Bitcask.open db_path, [:read_write]

    {:ok, %{
        db: db,
        events_count: do_count_all_events(db),
     }}
  end

  def handle_call({:save_events, events, stream_id}, _from, state) do
    %{db: db, events_count: events_count} = state
    {result, state} =
      case do_save_events(events, stream_id, db, events_count) do
        {:ok, events_count} ->
          {:ok, %{state | events_count: events_count}}
        {:error, reason} ->
          {{:error, reason}, state}
      end

    {:reply, result, state}
  end

  def handle_call(:load_all_events, _from, state = %{db: db}) do
    {:reply, do_load_all_events(db), state}
  end

  def handle_call(:count_all_events, _from, state = %{events_count: events_count}) do
    {:reply, events_count, state}
  end

  def handle_call({:load_stream_events, stream_id}, _from, state = %{db: db}) do
    {:reply, do_load_stream_events(stream_id, db), state}
  end

  def handle_call(:list_streams, _from, state = %{db: db}) do
    {:replu, do_list_streams(db), state}
  end


  #################################
  # Private API

  defp do_save_events(events, stream_id, db, events_count) do
    events_count =
      events
      |> Enum.reduce(events_count, fn event, order_id ->
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

    {:ok, events_count}
  end

  defp do_load_all_events(db) do
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

  defp do_count_all_events(db) do
    Bitcask.stream_keys(db)
    |> Stream.map(&:erlang.binary_to_term/1)
    |> Enum.filter(fn
      {:event, _event_id, _stream_id, _order_id} -> true
      _ -> false
    end)
    |> length
  end

  defp do_load_stream_events(stream_id, db) do
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
      :not_found -> {:ok, []}
      err ->
        {:error, err}
    end
  end

  defp do_list_streams(db) do
    streams = Bitcask.fold_keys(db, &fold_stream_keys/2, [])
    {:ok, streams}
  end

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
