#alias ExAws.S3

defmodule Receiver do
  def wait_for_messages do
    queue_name = "to_process_w2"
    exchange_name = "ex_w2"
    {:ok, connection} = AMQP.Connection.open("amqp://tineo:tineo@104.131.75.179")
    {:ok, channel} = AMQP.Channel.open(connection)

    AMQP.Queue.declare(channel, queue_name, durable: true)
    AMQP.Exchange.direct(channel, exchange_name, durable: true)
    AMQP.Queue.bind(channel, queue_name, exchange_name)
    #AMQP.Basic.qos(channel, prefetch_count: 1)
    AMQP.Basic.consume(channel, queue_name, nil, no_ack: false)

    Agent.start_link(fn -> [] end, name: :batcher)
    _wait_for_messages(channel)
  end

  defp _wait_for_messages(channel) do
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

        Map.put(idx_row, :sysid, row["Matrix_Unique_ID"]) |> IO.inspect
        Map.put(idx_row, :mls_num, row["MLSNumber"]) |> IO.inspect #mls_num
        Map.put(idx_row, :date_property, row["OriginalEntryTimestamp"])
            |> IO.inspect #date_property

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

        case row["PropertyType"] do
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

        end |>IO.inspect  #class_id

        IO.puts row["PropertyType"]
        IO.puts row["PropTypeTypeofBuilding"]

        IO.puts "2" #board_id
        ## SELECT `id` FROM idx_city WHERE `name` = City limit 1
        IO.puts row["City"] #city_id
        ## SELECT `id` FROM idx_office WHERE `code` = ListOfficeMLSID limit 1
        IO.puts row["ListOfficeMLSID"] #office_id
        ## SELECT `id` FROM idx_agent WHERE `code` = ListAgentMLSID limit 1
        IO.puts row["ListAgentMLSID"] #agent_id
        ## SELECT `id` FROM idx_agent WHERE `code` = CoListAgentMLSID limit 1
        IO.puts row["CoListAgentMLSID"] #co_agent_id

        IO.puts row["UnitNumber"]

        ## IF ( UnitNumber <> "", CONCAT_WS(" ", StreetNumber, StreetDirPrefix, StreetName, "#", UnitNumber ), CONCAT_WS(" ", StreetNumber, StreetDirPrefix, StreetName ) ) AS address_short,
        IO.puts [row["StreetNumber"], " ", "StreetDirPrefix"," ","StreetName"] #address_short
        ## CONCAT_WS(" ", City, ", FL", PostalCode) AS address_large,
        IO.puts [row["City"]," ", ", FL"," ",row["PostalCode"]] #address_large

        IO.puts row["OriginalListPrice"] #price_origin
        IO.puts row["ListPrice"] #price

        if (row["PropertyType"] in ["Single Family","Condo/Co-Op/Villa/Townhouse","Residential Rental"]),
           do: 0,else: 1 |> IO.puts #is_commercial

        if (row["PropertyType"] == "Residential Rental"),
           do: 1, else: (if (row["TypeofProperty"] == "lease"), do: 1, else: 0) |> IO.puts #is_rental

        IO.puts row["YearBuilt"] #year
        IO.puts row["BedsTotal"] #bed
        IO.puts row["BathsFull"] #bath
        IO.puts row["BathsHalf"] #baths_half

        IO.puts row["PhotoCount"] #img_cnt
        if ( String.to_integer(row["PhotoCount"]) > 0 ), do: [row["MLSNumber"],"_","1.jpg"], else: [""] |> IO.puts #image
        IO.puts row["PhotoModificationTimestamp"] #img_date

        IO.puts row["StreetNumber"] #st_number
        IO.puts row["StreetName"] #st_name
        IO.puts row["UnitNumber"] #unit
        IO.puts row["PostalCode"] #zip
        IO.puts row["SqFtLivArea"] #sqft
        IO.puts row["SqFtTotal"] #lot_size
        IO.puts row["LotDescription"] #lot_desc

        if (row["ParkingDescription"] != ""),do: 1,else: 0 |> IO.puts #parking
        IO.puts row["ParkingDescription"] #parking_desc
        IO.puts row["LegalDescription"] #legal_desc`,
        ##SELECT `id` FROM idx_county WHERE `name` = CountyOrParish limit 1
        if (row["CountyOrParish"] != "") ,do: 666,else: 0 |> IO.puts #county_id

        IO.puts row["WaterfrontDescription"] #wv
        IO.puts row["WaterAccess"] #wa
        IO.puts row["Area"] #area
        IO.puts "" #more_info
        IO.puts "" #condo_unit

        IO.puts row["UnitFloorLocation"] #condo_floor
        IO.puts row["SubdivisionName"] #subdivision
        IO.puts row["SubdivisionComplexBldg"] #complex
        IO.puts row["DevelopmentName"] #development
        if ( String.contains?(row["Remarks"], "hotel")), do: 1, else: 0 |> IO.puts #condo_hotel
        IO.puts row["REOYN"] #foreclosure
        ## IF ( CONCAT_WS(' ', Remarks, WaterAccess) regexp "boat dock|private dock", 1, 0) AS `boat_dock`,
        { _, pattern } = Regex.compile("boat dock|private dock")
          if ( Regex.match?( pattern,
               IO.iodata_to_binary([row["Remarks"], "' '", row["WaterAccess"]]))), do: 1, else: 0 |> IO.puts #boat_dock
        IO.puts  0 #water_view`
        IO.puts row["WaterfrontPropertyYN"] #water_front
        if (row["WaterfrontDescription"] == "Ocean Front"), do: 1, else: 0 |> IO.puts #ocean_front
        ## IF (Remarks LIKE "%pool%", 1, 0) AS `pool`,
        if ( String.contains?(row["Remarks"], "pool")), do: 1, else: 0 |> IO.puts #pool
        if (row["FurnishedInfoList"] == "Furnished"), do: 1, else: 0 |> IO.puts #furnished
        IO.puts row["PetsAllowedYN"] #pets

        if ( String.contains?(row["Remarks"], "penthouse")), do: 1, else: 0 |> IO.puts #penthouse

        if ( String.contains?(row["Remarks"], "townhouse")), do: 1, else: 0 |> IO.puts #tw
        if ( String.contains?(row["Remarks"], "golf")), do: 1, else: 0 |> IO.puts #golf
        if ( String.contains?(row["Remarks"], "tennis")), do: 1, else: 0 |> IO.puts #tennis

        IO.puts(row["ShortSaleYN"]) #short_sale
        if ( String.contains?(row["Remarks"], "house")), do: 1, else: 0 |> IO.puts #guest_house

        IO.puts 0 #oh
        if ( String.contains?(row["Remarks"], "gated")), do: 1, else: 0 |> IO.puts #gated_community,
        IO.puts 0 #unit_floor
        IO.puts row["ParcelNumber"] #folio_number

        IO.puts row["ParcelNumber"] #folio_number

        { _, pattern } = Regex.compile("-")
        #IF (	ParcelNumber REGEXP "-", SUBSTRING(ParcelNumber, 1, 2),	"" ) AS `fn_muni`,
        if ( Regex.match?( pattern, row["ParcelNumber"])),
           do: String.slice(row["ParcelNumber"], 0..1), else: "" |> IO.puts #fn_muni
        #IF (	ParcelNumber REGEXP "-", SUBSTRING(ParcelNumber, 4, 2),	"" ) AS `fn_area`,
        if ( Regex.match?( pattern, row["ParcelNumber"])),
           do: String.slice(row["ParcelNumber"], 3..4), else: "" |> IO.puts #fn_area
        #IF (	ParcelNumber REGEXP "-", SUBSTRING(ParcelNumber, 7, 2),	"") AS `fn_section`,
        if ( Regex.match?( pattern, row["ParcelNumber"])),
           do: String.slice(row["ParcelNumber"], 6..7), else: "" |> IO.puts #fn_section
        #IF (	ParcelNumber REGEXP "-", SUBSTRING(ParcelNumber, 10, 3), "") AS `fn_sub`,
        if ( Regex.match?( pattern, row["ParcelNumber"])),
           do: String.slice(row["ParcelNumber"], 9..12), else: "" |> IO.puts #fn_sub

        #CASE PropertyType
        #    WHEN "Single Family" THEN Style
        #    WHEN "Condo/Co-Op/Villa/Townhouse" THEN Style
        #    WHEN "Residential Rental" THEN Style
        #    ELSE PropTypeTypeofBuilding
        #END AS `style`,
        if ( row["PropertyType"]
             in ["Single Family", "Condo/Co-Op/Villa/Townhouse", "Residential Rental"]),
           do: row["Style"], else: row["PropTypeTypeofBuilding"] |> IO.puts #style
        IO.puts row["OriginalEntryTimestamp"] #date_create
        IO.puts row["MatrixModifiedDT"] #last_updated
        IO.puts "now()" #date_proccess

        #CASE Status
        #    WHEN "Active" THEN 1
        #    WHEN "Closed Sale" THEN 2
        #    WHEN "Backup Contract-Call LA" THEN 6
        #    WHEN "Pending Sale" THEN 6
        #    WHEN "Rented" THEN 5
        #    ELSE 0
        #    END  AS `status`,

        case row["Status"] do
          "Active" -> 1
          "Closed Sale" -> 2
          "Backup Contract-Call LA" -> 6
          "Pending Sale" -> 6
          "Rented" -> 5
          _ -> 0
        end |> IO.puts #status

        #IF ( UnitNumber <> "",
        #        REPLACE ( REPLACE ( lower( CONCAT_WS( "-", StreetNumber, StreetDirPrefix, StreetName, UnitNumber,	City, "FL",	PostalCode,	MLSNumber ) ), "#", ""), " ", "-"),
        #        REPLACE ( REPLACE ( lower( CONCAT_WS( "-", StreetNumber, StreetDirPrefix, StreetName, City, "FL", PostalCode, MLSNumber ) ), "#", "" ), " ", "-" )
        #) AS slug,
        #0 AS `adom`,

        (if ( row["UnitNumber"] != "" ),
           do: Enum.join([
             row["StreetNumber"],row["StreetDirPrefix"],row["StreetName"],row["UnitNumber"],
             row["City"],"FL",row["PostalCode"],row["MLSNumber"]
           ], "-") |> String.downcase |> String.replace(",", "-") |> String.replace(" ", "-") ,
             else: Enum.join([
               row["StreetNumber"],row["StreetDirPrefix"],row["StreetName"],
               row["City"],"FL",row["PostalCode"],row["MLSNumber"]
             ], "-") |> String.downcase |> String.replace(",", "-") |> String.replace(" ", "-")) |> IO.puts

        IO.puts row["lat"] #lat
        IO.puts row["lng"] #lng
        IO.puts row["Status"] #status_name
        IO.puts row["DOM"] #days_market
        IO.puts row["TypeofProperty"] #type_property
        IO.puts row["ListingType"] #listing_type
        IO.puts row["ConstructionType"] #construction
        IO.puts row["AssocFeePaidPer"] #assoc_fee_paid
        IO.puts row["UnitView"] #unit_view
        IO.puts row["FloorDescription"] #floor_desc
        IO.puts row["StyleofProperty"] #style
        IO.puts row["ParkingRestrictions"] #parking_restric
        IO.puts row["PetRestrictions"] #pets_restric
        IO.puts row["UnitFloorLocation"] #unit_building
        IO.puts row["TotalFloorsInBuilding"] #floor_building
        IO.puts row["StateOrProvince"] #state
        IO.puts row["TotalAcreage"] #acreage
        IO.puts row["RentPeriod"] #rent_period
        IO.puts row["EquipmentAppliances"] #appliance
        IO.puts row["DiningDescription"] #dining
        IO.puts row["CoolingDescription"] #cooling
        IO.puts row["Restrictions"] #restriction
        IO.puts row["SecurityInformation"] #security
        IO.puts row["TermsConsidered"] #terms
        IO.puts row["TypeofGoverningBodies"] #governing
        IO.puts row["WaterfrontFrontage"] #waterfront_frontage

        IO.puts row["UnitNumber"] #unit_number
        IO.puts row["TaxYear"] #tax_year
        IO.puts row["TaxAmount"] #tax_amount
        IO.puts row["Remarks"] #remark
        IO.puts row["ExteriorFeatures"] #feature_exterior
        IO.puts row["InteriorFeatures"] #feature_interior
        IO.puts row["Amenities"] #amenities
        IO.puts row["AssociationFee"] #assoc_fee
        IO.puts row["VirtualTour"] #virtual_tour
        IO.puts row["Area"] #area

        IO.puts row["Area"] #area

        #UPPER( CONCAT_WS(' ', StreetNumber, StreetDirPrefix, StreetName, City, 'FL', PostalCode )) AS address_map
        Enum.join([
          row["StreetNumber"],row["StreetDirPrefix"],row["StreetName"],
          row["City"],"FL",row["PostalCode"]
        ], " ") |> String.upcase |> IO.puts #address_map

        ## ACK if no errors
        AMQP.Basic.ack(channel, meta.delivery_tag)
        _wait_for_messages(channel)
    end
  end
end