defmodule Docomo.TracksController do
  use DocomoWeb, :controller

  def index(conn, params) do
    {:ok, message} = Poison.encode(params)
    Docomo.Worker.publish(message)
    conn
    |> text("200")
  end
end
