defmodule KafkexTest do
  use ExUnit.Case
  doctest Kafkex

  test "metadata request" do
    metadata = Kafkex.connect({'localhost', 19092}) |> Kafkex.metadata(["test0", "test1"])
    assert metadata == {[{0, "carlo-arch.localdomain", 19092}],
                         [{0, "test0", [{0, 1, 0, [0], [0]}, {0, 0, 0, [0], [0]}]},
                          {0, "test1", [{0, 1, 0, [0], [0]}, {0, 0, 0, [0], [0]}]}]}
  end
end
