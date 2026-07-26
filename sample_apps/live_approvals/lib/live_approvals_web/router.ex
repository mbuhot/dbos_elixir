defmodule LiveApprovalsWeb.Router do
  use LiveApprovalsWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {LiveApprovalsWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", LiveApprovalsWeb do
    pipe_through :browser

    live "/", RequestLive.Index, :index
    live "/requests", RequestLive.Index, :index
    live "/requests/:id", RequestLive.Show, :show
    live "/approvals", ApproverLive.Index, :index
  end
end
