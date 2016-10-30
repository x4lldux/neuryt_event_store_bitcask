defmodule Neuryt.EventStore.BitcaskTest do
  use ExUnit.Case
  alias Neuryt.EventStore.Bitcask

  setup do
    name =
      :rand.uniform
      |> :erlang.phash2
    path = "test/#{name}/"
    Application.put_env :neuryt_event_store_bitcask, :db_path, path
    {:ok, pid} = Bitcask.start_link

    on_exit fn ->
      File.rm_rf path
    end

    %{name: name, path: path, pid: pid}
  end

  test "opening database creates a directory with the path", %{path: path} do
    File.exists?(path)
  end

  test "an event can be saved to a stream", %{path: path} do
    event = "fake_event"
    event_enveloped = %{id: 1, event: event}
    stream_id = "fake_stream_id"
    assert :ok = Bitcask.save_event event_enveloped, stream_id

    data = File.read!(data_file(path))
    assert :binary.match(data, event) != :nomatch
    assert :binary.match(data, stream_id) != :nomatch
  end

  test "bunch of events can be saved to a stream", %{path: path} do
    events = for i <- 1..10 do
      event = "fake_event#{i}"
      event_enveloped = %{id: i, event: event}
      {event, event_enveloped}
    end

    stream_id = "fake_stream_id"
    assert :ok =
      events
      |> Enum.map(fn {_, e} -> e end)
      |> Bitcask.save_events(stream_id)

    data = File.read!(data_file(path))
    events
    |> Enum.each(fn {e, _ee} ->
      assert :binary.match(data, e) != :nomatch
    end)
    assert :binary.match(data, stream_id) != :nomatch
  end

  test "can load saved event from a stream" do
    event1 = "fake_event1"
    event1_enveloped = %{id: :rand.uniform, event: event1}
    stream1_id = :rand.uniform |> :erlang.phash2
    event2 = "fake_event2"
    event2_enveloped = %{id: :rand.uniform, event: event2}
    stream2_id = :rand.uniform |> :erlang.phash2
    event3 = "fake_event3"
    event3_enveloped = %{id: :rand.uniform, event: event3}

    assert :ok = Bitcask.save_event(event1_enveloped, stream1_id)
    assert :ok = Bitcask.save_event(event2_enveloped, stream2_id)
    assert :ok = Bitcask.save_event(event3_enveloped, stream1_id)

    assert Bitcask.load_stream_events(stream1_id) == {:ok, [event1_enveloped,
                                                            event3_enveloped]}
    assert Bitcask.load_stream_events(stream2_id) == {:ok, [event2_enveloped]}
  end

  test "can load all saved events" do
    event1 = "fake_event1"
    event1_enveloped = %{id: :rand.uniform, event: event1}
    stream1_id = :rand.uniform |> :erlang.phash2
    event2 = "fake_event2"
    event2_enveloped = %{id: :rand.uniform, event: event2}
    stream2_id = :rand.uniform |> :erlang.phash2
    event3 = "fake_event3"
    event3_enveloped = %{id: :rand.uniform, event: event3}
    stream3_id = :rand.uniform |> :erlang.phash2

    assert :ok = Bitcask.save_event(event1_enveloped, stream1_id)
    assert :ok = Bitcask.save_event(event2_enveloped, stream2_id)
    assert :ok = Bitcask.save_event(event3_enveloped, stream3_id)

    assert Bitcask.load_all_events() == {:ok, [
                                            event1_enveloped,
                                            event2_enveloped,
                                            event3_enveloped,
                                          ]}
  end

  test "can list all saved streams" do
    event1 = "fake_event1"
    event1_enveloped = %{id: :rand.uniform, event: event1}
    stream1_id = :rand.uniform |> :erlang.phash2
    event2 = "fake_event2"
    event2_enveloped = %{id: :rand.uniform, event: event2}
    stream2_id = :rand.uniform |> :erlang.phash2
    event3 = "fake_event3"
    event3_enveloped = %{id: :rand.uniform, event: event3}
    stream3_id = :rand.uniform |> :erlang.phash2

    assert :ok = Bitcask.save_event(event1_enveloped, stream1_id)
    assert :ok = Bitcask.save_event(event2_enveloped, stream2_id)
    assert :ok = Bitcask.save_event(event3_enveloped, stream3_id)

    assert {:ok, stream_ids} =  Bitcask.list_streams
    assert Enum.sort(stream_ids) == Enum.sort([stream1_id, stream2_id, stream3_id])
  end

  defp data_file(path) do
    path<>"1.bitcask.data"
  end
end
