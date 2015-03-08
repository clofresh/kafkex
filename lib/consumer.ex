defmodule Kafkex.Consumer do
  def start(callback) do
    socket = Kafkex.connect({'localhost', 19092})
    initial = %{{"test0", 0} => {0, 1024, 1024*1024}}
    callback.(1, 2, 3)
    run(socket, initial, callback)
  end

  def to_fetch_requests([{{topic, partition}, {offset, cur_max_bytes, _max_max_bytes}} |
                         fetch_state], topics) do
    topic_partitions = Map.get(topics, topic, [])
    to_fetch_requests(fetch_state, Map.put(topics, topic, topic_partitions
                                           ++ [{partition, offset, cur_max_bytes}]))
  end

  def to_fetch_requests([], topics) do
    Map.to_list(topics)
  end

  def run(_socket, fetch_state, _callback) when map_size(fetch_state) == 0 do
    IO.puts("Nothing to fetch, exiting")
  end

  def run(socket, fetch_state, callback) do
    fetch_requests = to_fetch_requests(Map.to_list(fetch_state), %{})
    {correlation_id, topics} = Kafkex.fetch(socket, fetch_requests, 100, 1024, 1000)

    new_fetch_requests = Enum.reduce(topics, fetch_state, fn({topic, partitions}, fetch_state) ->
      Enum.reduce(partitions, fetch_state, fn
        ({partition, _error, hwmark, []}, fetch_state) ->
          key = {topic, partition}
          {offset, cur_max_bytes, max_max_bytes} = Map.get(fetch_state, key)
          if offset + 1 < hwmark do # Didn't get messages even though not at the end
            if cur_max_bytes < max_max_bytes do
              # Still under the max_max_bytes, so double the cur_max_bytes
              Map.put(fetch_state, key, {offset, cur_max_bytes * 2, max_max_bytes})
            else
              # Drop this topic partition because there is a message that's bigger
              # than out max max bytes
              IO.puts("Message larger than #{max_max_bytes} bytes in #{topic}:#{partition}, removing from fetch list")
              Map.drop(fetch_state, [key])
            end
          else # We're at the end of the topic
            Map.put(fetch_state, key, {offset, cur_max_bytes, max_max_bytes})
          end
        ({partition, _error, _hwmark, messages}, fetch_state) ->
          key = {topic, partition}
          {_, cur_max_bytes, max_max_bytes} = Map.get(fetch_state, key)
          max_offset = Enum.reduce(messages, -1, fn({offset, message}, max_offset) ->
            if offset > max_offset do
              offset
            else
              max_offset
            end
          end)
          callback.(topic, partition, messages)
          Map.put(fetch_state, key, {max_offset + 1, cur_max_bytes, max_max_bytes})
        end)
    end)

    :timer.sleep(1000)
    run(socket, new_fetch_requests, callback)
  end

end
