defmodule DocomoWeb.Router do
  use DocomoWeb, :router

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/api", DocomoWeb do
    pipe_through :api

    post "/t", TracksController, :index
  end
end
