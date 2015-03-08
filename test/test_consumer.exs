Kafkex.Consumer.start(fn(topic, partition, messages) ->
  IO.inspect({topic, partition, messages})
end)
