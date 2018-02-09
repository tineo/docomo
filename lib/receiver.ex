#alias ExAws.S3

defmodule Receiver do
  def wait_for_messages do

    {:ok, rds} = Redix.start_link("redis://127.0.0.1:6379/3", name: :redix)

    queue_name = "to_process_w2"
    exchange_name = "ex_w2"
    {:ok, connection} = AMQP.Connection.open("amqp://tineo:tineo@104.131.75.179")
    {:ok, channel} = AMQP.Channel.open(connection)

    Redix.command(rds, ["SET", "value", "meow"])

    AMQP.Queue.declare(channel, queue_name, durable: true)
    AMQP.Exchange.direct(channel, exchange_name, durable: true)
    AMQP.Queue.bind(channel, queue_name, exchange_name)
    #AMQP.Basic.qos(channel, prefetch_count: 1)
    AMQP.Basic.consume(channel, queue_name, nil, no_ack: false)

    Agent.start_link(fn -> [] end, name: :batcher)
    _wait_for_messages(channel, rds)
  end

  defp reverse_put(value, map, key) do
    Map.put(map,key, value)
  end

  defp _wait_for_messages(channel, rds) do
    receive do
      {:basic_deliver, payload, meta} ->
        #IO.puts meta

        idx_row = Map.new()

        ###
        listing_more_info = ['days_market', 'type_property', 'listing_type','construction', 'assoc_fee_paid', 'status_name',
          'unit_view',  'floor_desc',  'style',  'parking_restric', 'pets_restric', 'unit_building',
          'floor_building', 'unit', 'state', 'acreage', 'lot_size', 'rent_period', 'appliance', 'dining',
          'cooling', 'restriction', 'security', 'terms', 'governing', 'waterfront_frontage'];

        IO.puts "received a message!"
        row = Poison.decode!(payload)

        idx_row =
          row["Matrix_Unique_ID"] #|> IO.inspect
          |> reverse_put(idx_row, :sysid)
        idx_row =
          row["MLSNumber"] #|> IO.inspect #mls_num
          |> reverse_put(idx_row, :mls_num)
        idx_row =
          row["OriginalEntryTimestamp"]# |> IO.inspect #date_property
          |> reverse_put(idx_row, :date_property)

        #"2017-10-15T14:54:50.537"
        row["OriginalEntryTimestamp"] |> IO.puts
        dt = String.split("2017-10-15T14:54:50.537", ".")
        str = List.first(dt) <> "Z"
        {:ok, datetime, 0} = DateTime.from_iso8601( str )
        unix = DateTime.to_unix(datetime)
        {entero, _} = Integer.parse(List.last(dt))
        unix + entero |> IO.puts

        #DateTime.from_iso8601(row["OriginalEntryTimestamp"])  |> IO.puts
        #|> DateTime.to_unix

        #CASE PropertyType
        #    WHEN "Single Family" THEN 2
        #    WHEN "Condo/Co-Op/Villa/Townhouse" THEN 1
        #    WHEN "Residential Rental" THEN IF (	UnitNumber != "", 1, 2 )
        #    WHEN "Residential Income" THEN 27
        #    WHEN "Residential Land/Boat Docks" THEN 26
        #    WHEN "Commercial/Business/Agricultural/Industrial Land" THEN 28
        #    WHEN "Business Opportunity" THEN 29
        #    ELSE
        #        CASE
        #           WHEN (PropTypeTypeofBuilding like "%industrial%") THEN 10
        #           WHEN (PropTypeTypeofBuilding like "%medical%") THEN 12
        #           WHEN (PropTypeTypeofBuilding like "%building%") THEN 6
        #           WHEN (PropTypeTypeofBuilding like "%income%") THEN 30
        #           WHEN (PropTypeTypeofBuilding like "%hotel%") THEN 20
        #           WHEN (PropTypeTypeofBuilding like "%service%") THEN 21
        #           WHEN (PropTypeTypeofBuilding like "%restaurant%") THEN 14
        #           WHEN (PropTypeTypeofBuilding like "%warehouse%") THEN 13
        #           WHEN (PropTypeTypeofBuilding like "%retail%") THEN 15
        #           WHEN (PropTypeTypeofBuilding like "%free%") THEN 16
        #           WHEN (PropTypeTypeofBuilding like "%school%") THEN 24
        #           WHEN (PropTypeTypeofBuilding like "%office%") THEN 22
        #           WHEN (PropTypeTypeofBuilding like "%shopping%") THEN 17
        #           WHEN (PropTypeTypeofBuilding like "%store%") THEN 18
        #           WHEN (PropTypeTypeofBuilding like "%adult%") THEN 3
        #           ELSE 19
        #        END
        #END AS class_id,

        idx_row =
        (case row["PropertyType"] do
          "Single Family" -> 1
          "Condo/Co-Op/Villa/Townhouse" -> 2
          "Residential Rental" -> 6
          "Residential Income" -> 6
          "Residential Land/Boat Docks" -> 5
          "Commercial/Business/Agricultural/Industrial Land" -> 5
          "Business Opportunity" -> 5
          _ -> cond do
                 String.contains?(row["PropTypeTypeofBuilding"], "industrial") -> 10
                 String.contains?(row["PropTypeTypeofBuilding"], "medical") -> 12
                 String.contains?(row["PropTypeTypeofBuilding"], "building") -> 6
                 String.contains?(row["PropTypeTypeofBuilding"], "income") -> 30
                 String.contains?(row["PropTypeTypeofBuilding"], "hotel") -> 20
                 String.contains?(row["PropTypeTypeofBuilding"], "service") -> 21
                 String.contains?(row["PropTypeTypeofBuilding"], "restaurant") -> 14
                 String.contains?(row["PropTypeTypeofBuilding"], "warehouse") -> 13
                 String.contains?(row["PropTypeTypeofBuilding"], "retail") -> 15
                 String.contains?(row["PropTypeTypeofBuilding"], "free") -> 16
                 String.contains?(row["PropTypeTypeofBuilding"], "school") -> 24
                 String.contains?(row["PropTypeTypeofBuilding"], "office") -> 22
                 String.contains?(row["PropTypeTypeofBuilding"], "shopping") -> 17
                 String.contains?(row["PropTypeTypeofBuilding"], "store") -> 18
                 String.contains?(row["PropTypeTypeofBuilding"], "adult") -> 3
                 true -> 19
               end

        end)
         |> reverse_put(idx_row, :class_id)
         #|>IO.inspect  #class_id

        #IO.puts
        idx_row =
         "2" #board_id
         |> reverse_put(idx_row, :board_id)

        ## SELECT `id` FROM idx_city WHERE `name` = City limit 1
        #IO.puts
        idx_row =
         row["City"] #city_id
         |> reverse_put(idx_row, :city_id)

        ## SELECT `id` FROM idx_office WHERE `code` = ListOfficeMLSID limit 1
        #IO.puts
        idx_row =
         row["ListOfficeMLSID"] #office_id
         |> reverse_put(idx_row, :office_id)

        ## SELECT `id` FROM idx_agent WHERE `code` = ListAgentMLSID limit 1
        #IO.puts
        idx_row =
         row["ListAgentMLSID"] #agent_id
         |> reverse_put(idx_row, :agent_id)

        ## SELECT `id` FROM idx_agent WHERE `code` = CoListAgentMLSID limit 1
        #IO.puts
        idx_row =
         row["CoListAgentMLSID"] #co_agent_id
         |> reverse_put(idx_row, :co_agent_id)


        ## IF ( UnitNumber <> "", CONCAT_WS(" ", StreetNumber, StreetDirPrefix, StreetName, "#", UnitNumber ), CONCAT_WS(" ", StreetNumber, StreetDirPrefix, StreetName ) ) AS address_short,
        #IO.puts
        idx_row =
         (if (row["UnitNumber"] != ""),
          do: Enum.join( [row["StreetNumber"],row["StreetDirPrefix"],row["StreetName"],"#",row["UnitNumber"]], " "),
          else: Enum.join([row["StreetNumber"], row["StreetDirPrefix"], row["StreetName"]]," " )) #address_short
         |> reverse_put(idx_row, :address_short)

        #CONCAT_WS(" ", City, ", FL", PostalCode) AS address_large,
        #IO.puts
        idx_row =
          Enum.join([row["City"], ", FL",row["PostalCode"]]," ") #address_large
         |> reverse_put(idx_row, :address_large)

        #IO.puts
        idx_row =
         row["OriginalListPrice"] #price_origin
         |> reverse_put(idx_row, :price_origin)

        #IO.puts
        idx_row =
         row["ListPrice"] #price
         |> reverse_put(idx_row, :price)

        idx_row =
         (if (row["PropertyType"] in ["Single Family","Condo/Co-Op/Villa/Townhouse","Residential Rental"]),
           do: 0,else: 1) #|> IO.puts #is_commercial
         |> reverse_put(idx_row, :is_commercial)

        idx_row =
         (if (row["PropertyType"] == "Residential Rental" ),
           do: 1, else: (if (row["TypeofProperty"] == "lease"), do: 1, else: 0)) #|> IO.puts #is_rental

        |> reverse_put(idx_row, :is_rental)

        #IO.puts
        idx_row =
         row["YearBuilt"] #year
         |> reverse_put(idx_row, :year)

        #IO.puts
        idx_row =
         row["BedsTotal"] #bed
         |> reverse_put(idx_row, :year)

        #IO.puts
        idx_row =
         row["BathsFull"] #bath
         |> reverse_put(idx_row, :bath)

        #IO.puts
        idx_row =
         row["BathsHalf"] #baths_half
         |> reverse_put(idx_row, :baths_half)

        #IO.puts
        idx_row =
         row["PhotoCount"] #img_cnt
         |> reverse_put(idx_row, :img_cnt)

        idx_row =
         (if ( String.to_integer(row["PhotoCount"]) > 0 ), do: Enum.join([row["MLSNumber"],"_","1.jpg"],""), else: "")
         #|> IO.puts #image
         |> reverse_put(idx_row, :image1)

        #IO.puts
        idx_row =
         row["PhotoModificationTimestamp"] #img_date
         |> reverse_put(idx_row, :img_date1)

        #IO.puts
        idx_row =
         row["StreetNumber"] #st_number
         |> reverse_put(idx_row, :st_number)

        #IO.puts
        idx_row =
         row["StreetName"] #st_name
         |> reverse_put(idx_row, :st_name)

        #IO.puts
        idx_row =
         row["UnitNumber"] #unit
         |> reverse_put(idx_row, :unit)

        #IO.puts
        idx_row =
         row["PostalCode"] #zip
         |> reverse_put(idx_row, :zip)

        #IO.puts
        idx_row =
         row["SqFtLivArea"] #sqft
         |> reverse_put(idx_row, :sqft)

        #IO.puts
        idx_row =
         row["SqFtTotal"] #lot_size
         |> reverse_put(idx_row, :lot_size)

        #IO.puts
        idx_row =
         row["LotDescription"] #lot_desc
         |> reverse_put(idx_row, :lot_desc)

        idx_row =
         (if (row["ParkingDescription"] != ""), do: 1,else: 0)
         #|> IO.puts #parking
         |> reverse_put(idx_row, :parking)

        #IO.puts
        idx_row =
         row["ParkingDescription"] #parking_desc
         |> reverse_put(idx_row, :parking_desc)

        #IO.puts
        idx_row =
         row["LegalDescription"] #legal_desc`,
         |> reverse_put(idx_row, :parking_desc)


        ##SELECT `id` FROM idx_county WHERE `name` = CountyOrParish limit 1
        idx_row =
         (if (row["CountyOrParish"] != "") ,do: 666,else: 0)
         #|> IO.puts #county_id
         |> reverse_put(idx_row, :county_id)

        #IO.puts
        idx_row =
         row["WaterfrontDescription"] #wv
         |> reverse_put(idx_row, :wv)

        #IO.puts
        idx_row =
         row["WaterAccess"] #wa
         |> reverse_put(idx_row, :wa)

        #IO.puts
        idx_row =
         row["Area"] #area
         |> reverse_put(idx_row, :area)

        #IO.puts
        idx_row = "" #more_info
         |> reverse_put(idx_row, :more_info)

        #IO.puts
        idx_row = "" #condo_unit
        |> reverse_put(idx_row, :condo_unit)

        #IO.puts
        idx_row =
         row["UnitFloorLocation"] #condo_floor
         |> reverse_put(idx_row, :condo_floor)

        #IO.puts
        idx_row =
         row["SubdivisionName"] #subdivision
         |> reverse_put(idx_row, :subdivision)

        #IO.puts
        idx_row =
         row["SubdivisionComplexBldg"] #complex
         |> reverse_put(idx_row, :complex)

        #IO.puts
        idx_row =
         row["DevelopmentName"] #development
         |> reverse_put(idx_row, :development)

        idx_row =
         (if ( String.contains?(row["Remarks"], "hotel")), do: 1, else: 0)
         # |> IO.puts #condo_hotel
         |> reverse_put(idx_row, :condo_hotel)

        #IO.puts
        idx_row =
          row["REOYN"] #foreclosure
          |> reverse_put(idx_row, :foreclosure)

        ## IF ( CONCAT_WS(' ', Remarks, WaterAccess) regexp "boat dock|private dock", 1, 0) AS `boat_dock`,
        { _, pattern } = Regex.compile("boat dock|private dock")
        idx_row =
          (if ( Regex.match?( pattern,
               Enum.join([row["Remarks"], "' '", row["WaterAccess"]]))), do: 1, else: 0)
        # |> IO.puts #boat_dock
        |> reverse_put(idx_row, :boat_dock)

        #IO.puts
        idx_row =  0 #water_view
         |> reverse_put(idx_row, :water_view)

        #IO.puts
        idx_row =
         row["WaterfrontPropertyYN"] #water_front
         |> reverse_put(idx_row, :water_front)

        idx_row =
         (if (row["WaterfrontDescription"] == "Ocean Front"), do: 1, else: 0)
         |> reverse_put(idx_row, :ocean_front)
         #|> IO.puts #ocean_front

        ## IF (Remarks LIKE "%pool%", 1, 0) AS `pool`,
        idx_row =
         (if ( String.contains?(row["Remarks"], "pool")), do: 1, else: 0)
         #|> IO.puts #pool
         |> reverse_put(idx_row, :pool)

        idx_row =
         (if (row["FurnishedInfoList"] == "Furnished"), do: 1, else: 0)
        #|> IO.puts #furnished
        |> reverse_put(idx_row, :furnished)

        #IO.puts
        idx_row =
         row["PetsAllowedYN"] #pets
         |> reverse_put(idx_row, :pets)

        idx_row =
         (if ( String.contains?(row["Remarks"], "penthouse")), do: 1, else: 0)
         |> reverse_put(idx_row, :penthouse)
        #|> IO.puts #penthouse

        idx_row =
         (if ( String.contains?(row["Remarks"], "townhouse")), do: 1, else: 0)
         |> reverse_put(idx_row, :tw)
        #|> IO.puts #tw

        idx_row =
         (if ( String.contains?(row["Remarks"], "golf")), do: 1, else: 0)
         |> reverse_put(idx_row, :golf)
        #|> IO.puts #golf

        idx_row =
         (if ( String.contains?(row["Remarks"], "tennis")), do: 1, else: 0)
         |> reverse_put(idx_row, :tennis)
        #|> IO.puts #tennis

        #IO.puts
        idx_row =
         row["ShortSaleYN"] #short_sale
         |> reverse_put(idx_row, :short_sale)

        idx_row =
         (if ( String.contains?(row["Remarks"], "house")), do: 1, else: 0)
         |> reverse_put(idx_row, :guest_house)
        #|> IO.puts #guest_house

        #IO.puts
        idx_row =
         0 #oh
         |> reverse_put(idx_row, :oh)

        idx_row =
         (if ( String.contains?(row["Remarks"], "gated")), do: 1, else: 0)
         #|> IO.puts #gated_community,
         |> reverse_put(idx_row, :gated_community)

        #IO.puts
        idx_row =
         0 #unit_floor
         |> reverse_put(idx_row, :unit_floor)

        #IO.puts
        idx_row =
         row["ParcelNumber"] #folio_number
         |> reverse_put(idx_row, :folio_number)

        { _, pattern } = Regex.compile("-")
        #IF (	ParcelNumber REGEXP "-", SUBSTRING(ParcelNumber, 1, 2),	"" ) AS `fn_muni`,
        idx_row =
         (if ( Regex.match?( pattern, row["ParcelNumber"])),
           do: String.slice(row["ParcelNumber"], 0..1), else: "") #|> IO.puts #fn_muni
         |> reverse_put(idx_row, :fn_muni)

        #IF (	ParcelNumber REGEXP "-", SUBSTRING(ParcelNumber, 4, 2),	"" ) AS `fn_area`,
        idx_row =
         (if ( Regex.match?( pattern, row["ParcelNumber"])),
           do: String.slice(row["ParcelNumber"], 3..4), else: "") #|> IO.puts #fn_area
           |> reverse_put(idx_row, :fn_area)

        #IF (	ParcelNumber REGEXP "-", SUBSTRING(ParcelNumber, 7, 2),	"") AS `fn_section`,
        idx_row =
         (if ( Regex.match?( pattern, row["ParcelNumber"])),
           do: String.slice(row["ParcelNumber"], 6..7), else: "") #|> IO.puts #fn_section
         |> reverse_put(idx_row, :fn_section)

        #IF (	ParcelNumber REGEXP "-", SUBSTRING(ParcelNumber, 10, 3), "") AS `fn_sub`,
        idx_row =
         (if ( Regex.match?( pattern, row["ParcelNumber"])),
           do: String.slice(row["ParcelNumber"], 9..12), else: "") #|> IO.puts #fn_sub
         |> reverse_put(idx_row, :fn_sub)

        #CASE PropertyType
        #    WHEN "Single Family" THEN Style
        #    WHEN "Condo/Co-Op/Villa/Townhouse" THEN Style
        #    WHEN "Residential Rental" THEN Style
        #    ELSE PropTypeTypeofBuilding
        #END AS `style`,
        idx_row =
        (if ( row["PropertyType"]
             in ["Single Family", "Condo/Co-Op/Villa/Townhouse", "Residential Rental"]),
           do: row["Style"], else: row["PropTypeTypeofBuilding"]) #|> IO.puts #style
         |> reverse_put(idx_row, :style)

        #IO.puts
        idx_row =
         row["OriginalEntryTimestamp"] #date_create
         |> reverse_put(idx_row, :date_create)

        #IO.puts
        idx_row =
         row["MatrixModifiedDT"] #last_updated
         |> reverse_put(idx_row, :last_updated)

        #IO.puts
        idx_row =
         "now()" #date_proccess
         |> reverse_put(idx_row, :date_proccess)
        #CASE Status
        #    WHEN "Active" THEN 1
        #    WHEN "Closed Sale" THEN 2
        #    WHEN "Backup Contract-Call LA" THEN 6
        #    WHEN "Pending Sale" THEN 6
        #    WHEN "Rented" THEN 5
        #    ELSE 0
        #    END  AS `status`,
        idx_row =
         (case row["Status"] do
          "Active" -> 1
          "Closed Sale" -> 2
          "Backup Contract-Call LA" -> 6
          "Pending Sale" -> 6
          "Rented" -> 5
          _ -> 0
         end) #|> IO.puts #status
         |> reverse_put(idx_row, :status)
        #IF ( UnitNumber <> "",
        #        REPLACE ( REPLACE ( lower( CONCAT_WS( "-", StreetNumber, StreetDirPrefix, StreetName, UnitNumber,	City, "FL",	PostalCode,	MLSNumber ) ), "#", ""), " ", "-"),
        #        REPLACE ( REPLACE ( lower( CONCAT_WS( "-", StreetNumber, StreetDirPrefix, StreetName, City, "FL", PostalCode, MLSNumber ) ), "#", "" ), " ", "-" )
        #) AS slug,
        #0 AS `adom`,
        idx_row =
         (if ( row["UnitNumber"] != "" ),
           do: Enum.join([
             row["StreetNumber"],row["StreetDirPrefix"],row["StreetName"],row["UnitNumber"],
             row["City"],"FL",row["PostalCode"],row["MLSNumber"]
           ], "-") |> String.downcase |> String.replace(",", "-") |> String.replace(" ", "-") ,
             else: Enum.join([
               row["StreetNumber"],row["StreetDirPrefix"],row["StreetName"],
               row["City"],"FL",row["PostalCode"],row["MLSNumber"]
             ], "-") |> String.downcase |> String.replace(",", "-") |> String.replace(" ", "-")) #|> IO.puts
         |> reverse_put(idx_row, :adom)

        #IO.puts
        idx_row =
         row["lat"] #lat
         |> reverse_put(idx_row, :lat)

        #IO.puts
        idx_row =
         row["lng"] #lng
         |> reverse_put(idx_row, :lng)

        #IO.puts
        idx_row =
         row["Status"] #status_name
         |> reverse_put(idx_row, :status_name)

        #IO.puts
        idx_row =
         row["DOM"] #days_market
         |> reverse_put(idx_row, :days_market)

        #IO.puts
        idx_row =
         row["TypeofProperty"] #type_property
         |> reverse_put(idx_row, :type_property)

        #IO.puts
        idx_row =
         row["ListingType"] #listing_type
         |> reverse_put(idx_row, :listing_type)

        #IO.puts
        idx_row =
         row["ConstructionType"] #construction
         |> reverse_put(idx_row, :construction)

        #IO.puts
        idx_row =
         row["AssocFeePaidPer"] #assoc_fee_paid
         |> reverse_put(idx_row, :assoc_fee_paid)

        #IO.puts
        idx_row =
         row["UnitView"] #unit_view
         |> reverse_put(idx_row, :unit_view)

        #IO.puts
        idx_row =
         row["FloorDescription"] #floor_desc
         |> reverse_put(idx_row, :floor_desc)

        #IO.puts
        idx_row =
         row["StyleofProperty"] #style
         |> reverse_put(idx_row, :style)

        #IO.puts
        idx_row =
         row["ParkingRestrictions"] #parking_restric
         |> reverse_put(idx_row, :parking_restric)

        #IO.puts
        idx_row =
         row["PetRestrictions"] #pets_restric
         |> reverse_put(idx_row, :pets_restric)

        #IO.puts
        idx_row =
         row["UnitFloorLocation"] #unit_building
         |> reverse_put(idx_row, :unit_building)

        #IO.puts
        idx_row =
         row["TotalFloorsInBuilding"] #floor_building
         |> reverse_put(idx_row, :floor_building)

        #IO.puts
        idx_row =
         row["StateOrProvince"] #state
         |> reverse_put(idx_row, :state)

        #IO.puts
        idx_row =
         row["TotalAcreage"] #acreage
         |> reverse_put(idx_row, :acreage)

        #IO.puts
        idx_row =
         row["RentPeriod"] #rent_period
         |> reverse_put(idx_row, :rent_period)

        #IO.puts
        idx_row =
         row["EquipmentAppliances"] #appliance
         |> reverse_put(idx_row, :appliance)

        #IO.puts
        idx_row =
         row["DiningDescription"] #dining
         |> reverse_put(idx_row, :dining)

        #IO.puts
        idx_row =
         row["CoolingDescription"] #cooling
         |> reverse_put(idx_row, :cooling)

        #IO.puts
        idx_row =
         row["Restrictions"] #restriction
         |> reverse_put(idx_row, :restriction)

        #IO.puts
        idx_row =
         row["SecurityInformation"] #security
         |> reverse_put(idx_row, :security)

        #IO.puts
        idx_row =
         row["TermsConsidered"] #terms
         |> reverse_put(idx_row, :terms)

        #IO.puts
        idx_row =
         row["TypeofGoverningBodies"] #governing
         |> reverse_put(idx_row, :governing)

        #IO.puts
        idx_row =
         row["WaterfrontFrontage"] #waterfront_frontage
         |> reverse_put(idx_row, :waterfront_frontage)

        #IO.puts
        idx_row =
         row["UnitNumber"] #unit_number
         |> reverse_put(idx_row, :unit_number)

        #IO.puts
        idx_row =
         row["TaxYear"] #tax_year
         |> reverse_put(idx_row, :tax_year)

        #IO.puts
        idx_row =
         row["TaxAmount"] #tax_amount
         |> reverse_put(idx_row, :tax_amount)

        #IO.puts
        idx_row =
         row["Remarks"] #remark
         |> reverse_put(idx_row, :remark)

        #IO.puts
        idx_row =
         row["ExteriorFeatures"] #feature_exterior
         |> reverse_put(idx_row, :feature_exterior)

        #IO.puts
        idx_row =
         row["InteriorFeatures"] #feature_interior
         |> reverse_put(idx_row, :feature_interior)

        #IO.puts
        idx_row =
         row["Amenities"] #amenities
         |> reverse_put(idx_row, :amenities)

        #IO.puts
        idx_row =
         row["AssociationFee"] #assoc_fee
         |> reverse_put(idx_row, :assoc_fee)

        #IO.puts
        idx_row =
         row["VirtualTour"] #virtual_tour
         |> reverse_put(idx_row, :virtual_tour)

        #IO.puts
        idx_row =
         row["Area"] #area
         |> reverse_put(idx_row, :area)


        #UPPER( CONCAT_WS(' ', StreetNumber, StreetDirPrefix, StreetName, City, 'FL', PostalCode )) AS address_map
        idx_row =
         Enum.join([
          row["StreetNumber"],row["StreetDirPrefix"],row["StreetName"],
          row["City"],"'FL'",row["PostalCode"]
         ], "' '") |> String.upcase #|> IO.puts #address_map
         |> reverse_put(idx_row, :address_map)

        #IO.inspect(idx_row)
        IO.inspect(idx_row[:status])
        {_, valor} = Redix.command(rds, ["GET", "value"])
        IO.puts valor
        ## ACK if no errors
        AMQP.Basic.ack(channel, meta.delivery_tag)
        _wait_for_messages(channel, rds)
    end
  end
end