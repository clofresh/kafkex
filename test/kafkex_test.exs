defmodule KafkexTest do
  use ExUnit.Case
  doctest Kafkex

  test "metadata request" do
    socket = Kafkex.connect({'localhost', 19092})
    try do
      metadata = Kafkex.metadata(socket, ["test0", "test1"], 1000, 10)
      assert metadata == {10, [{0, "carlo-arch.localdomain", 19092}],
                           [{0, "test0", [{0, 1, 0, [0], [0]}, {0, 0, 0, [0], [0]}]},
                            {0, "test1", [{0, 1, 0, [0], [0]}, {0, 0, 0, [0], [0]}]}]}
    after
      :gen_tcp.close(socket)
    end
  end

  test "produce then fetch" do
    socket = Kafkex.connect({'localhost', 19092})
    try do
      response = Kafkex.produce(socket, [{"test0", [{0, ["yo", "sup"]},
                                                    {1, ["not much"]}]},
                                          {"test1", [{1, ["blah"]}]}],
                                1, 1000)
      [{"test1", [{1, 0, o1}]}, {"test0", [{1, 0, o01}, {0, 0, o00}]}] = response

      max_bytes = 1024
      messages = Kafkex.fetch(socket, [{"test0", [{0, o00, max_bytes}, {1, o01, max_bytes}]},
                                       {"test1", [{1, o1, max_bytes}]}],
                              1000, 1024, 5000, 12)
      assert messages == {12,
            [{"test1", [{1, 0, o1 + 1, [{o1, "blah"}]}]},
             {"test0",
              [{0, 0, o00 + 2, [{o00, "yo"}, {o00 + 1, "sup"}]}, {1, 0, o01 + 1, [{o01, "not much"}]}]}]}


    after
      :gen_tcp.close(socket)
    end
  end
end
