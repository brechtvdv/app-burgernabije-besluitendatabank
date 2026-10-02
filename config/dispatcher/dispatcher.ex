defmodule Dispatcher do
  use Matcher
  define_accept_types [
    html: [ "text/html", "application/xhtml+html" ],
    json: [ "application/json", "application/vnd.api+json", "application/sparql-results+json" ]
  ]

  @any %{}
  @json %{ accept: %{ json: true } }
  @html %{ accept: %{ html: true } }

  # In order to forward the 'themes' resource to the
  # resource service, use the following forward rule:
  #
  # match "/themes/*path", @json do
  #   Proxy.forward conn, path, "http://resource/themes/"
  # end
  #
  # Run `docker-compose restart dispatcher` after updating
  # this file.

  #############################################################
  # IMPORTANT NOTES
  # Resource calls are wired through mu-cache again.
  #############################################################

  ###############
  # RESOURCES
  ###############
  get "/articles/*path", @any do
    Proxy.forward conn, path, "http://cache/articles/"
  end

  get "/administrative-units/*path", @any do
    Proxy.forward conn, path, "http://cache/administrative-units/"
  end

  get "/administrative-unit-classification-codes/*path", @any do
    Proxy.forward conn, path, "http://cache/administrative-unit-classification-codes/"
  end

  get "/agenda-item-handlings/*path", @any do
    Proxy.forward conn, path, "http://cache/agenda-item-handlings/"
  end

  get "/agenda-items/*path", @any do
    Proxy.forward conn, path, "http://cache/agenda-items/"
  end

  get "/governing-bodies/*path", @any do
    Proxy.forward conn, path, "http://cache/governing-bodies/"
  end

  get "/governing-body-classification-codes/*path", @any do
    Proxy.forward conn, path, "http://cache/governing-body-classification-codes/"
  end

  get "/locations/*path", @any do
    Proxy.forward conn, path, "http://cache/locations/"
  end

  get "/mandataries/*path", @any do
    Proxy.forward conn, path, "http://cache/mandataries/"
  end

  get "/memberships/*path", @any do
    Proxy.forward conn, path, "http://cache/memberships/"
  end

  get "/resolutions/*path", @any do
    Proxy.forward conn, path, "http://cache/resolutions/"
  end

  get "/sessions/*path", @any do
    Proxy.forward conn, path, "http://cache/sessions/"
  end

  get "/votes/*path", @any do
    Proxy.forward conn, path, "http://cache/votes/"
  end

  ###############
  # SERVICES
  ###############

  # to generate uuids manually
  match "/uuid-generation/run/*_path", @json do
    Proxy.forward conn, [], "http://uuid-generation/run"
  end

  match "/rdfa-harvest/*path", @any do
    Proxy.forward conn, path, "http://rdfa-harvest/"
  end

  #################
  # prometheus reporting
  #################
  get "/metrics", @any do
    Proxy.forward conn, [], "http://metrics/metrics"
  end


  ###############################################################
  # SEARCH
  ###############################################################

  match "/search/*path", @json do
    Proxy.forward conn, path, "http://search/"
  end

  #################################################################
  #  Exports
  #################################################################

  match "/generate-reports/*path", @any do
    Proxy.forward conn, path, "http://report-generation/"
  end

  get "/download-exports/*path", @any do
    Proxy.forward conn, path, "http://download-exports/"
  end

  ###############
  # FRONTEND
  ###############
  match "/assets/*path", @any do
    Proxy.forward conn, path, "http://frontend/assets/"
  end

  match "/@appuniversum/*path", @any do
    Proxy.forward conn, path, "http://frontend/@appuniversum/"
  end

  match "/*_path", @html do
    Proxy.forward conn, [], "http://frontend/index.html"
  end

  #################
  # NOT FOUND
  #################
  match "/*_", %{ last_call: true } do
    send_resp( conn, 404, "Route not found.  See config/dispatcher.ex" )
  end
end
