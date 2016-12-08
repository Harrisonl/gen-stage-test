alias Experimental.GenStage

defmodule Go do
  def go do
    {:ok, producer} = GenStage.from_enumerable(File.stream!("nasa.log"))
    {:ok, consumer1} = GsConsumer.start_link("1")
    {:ok, consumer2} = GsConsumer.start_link("2")
    {:ok, consumer3} = GsConsumer.start_link("3")
    {:ok, consumer4} = GsConsumer.start_link("4")

    GenStage.sync_subscribe(consumer1, to: producer)
    GenStage.sync_subscribe(consumer2, to: producer)
    GenStage.sync_subscribe(consumer3, to: producer)
    GenStage.sync_subscribe(consumer4, to: producer)
  end

end

defmodule GsProducer do
  use GenStage

  def start_link(file) do
    IO.inspect file
    GenStage.start_link(__MODULE__, file, name: __MODULE__)
  end

  def init(file) do
    stream = File.stream! file
    {:producer, stream}
  end

  def handle_demand(demand, stream) when demand > 0 do
    lines = stream |> Stream.take(demand) |> Enum.to_list
    {:noreply, lines, Stream.drop(stream, demand)}
  end
end

defmodule GsConsumer do
  use GenStage

  def start_link(num) do
    GenStage.start_link(__MODULE__, [num])
  end

  def init([num]) do
    {:consumer, num}
  end

  def handle_events(lines, _from, num) do
    File.open("results-#{num}.log", [:append], fn(file) ->
      lines
      |> Enum.map(fn(line) ->
        << head::binary-size(1), rest::binary>> = line
        head
      end)
      |> Enum.map(fn(x) ->
        IO.binwrite(file, x)
      end)
    end)
    {:noreply, [], nil}
  end
end
