(*---------------------------------------------------------------------------
   Copyright (c) 2016 Vincent Bernardoff. All rights reserved.
   Distributed under the ISC license, see terms at the end of the file.
   %%NAME%% %%VERSION%%
  ---------------------------------------------------------------------------*)

(* DTC to BitMEX simple bridge *)

open Core
open Async
open Cohttp_async

open Bs_devkit
open Bitmex_types
open Bmex
open Bmex_common

module DTC = Dtc_pb.Dtcprotocol_piqi
module WS = Bmex_ws
module REST = Bmex_rest

let trade_accountf ~userid ~username =
  username ^ ":" ^ Int.to_string userid

let cut_trade_account = function
| "" -> None
| s -> match String.split s ~on:':' with
| [username; id] -> Some (username, Int.of_string id)
| _ -> invalid_arg "cut_trade_account"

let use_testnet = ref false
let base_uri = ref @@ Uri.of_string "https://www.bitmex.com"
let my_exchange = ref "BMEX"

let log_bitmex = Log.create ~level:`Error ~on_error:`Raise ~output:Log.Output.[stderr ()]
let log_dtc = Log.create ~level:`Error ~on_error:`Raise ~output:Log.Output.[stderr ()]
let log_ws = Log.create ~level:`Error ~on_error:`Raise ~output:Log.Output.[stderr ()]

let ws_feed_connected : unit Condition.t = Condition.create ()

module ApiKey = struct
  module T = struct
    type t = {
      id : string ;
      userId : int ;
      secret : string ;
      username : string ;
    } [@@deriving sexp]

    let compare { id } { id = id2 } = String.compare id id2

    let create ~id ~userId ~secret ~username =
      { id ; userId ; secret ; username }

    let of_rest { REST.ApiKey.id; secret; permissions; enabled; userId } =
      match enabled with
      | false -> None
      | true ->
          List.find_map permissions ~f:begin function
          | Perm _ -> None
          | Dtc username ->
              Some (create ~id ~secret ~userId ~username)
          end
  end

  include T
  module Set = struct
    include Set.Make(T)

    let of_rest entries =
      List.fold_left entries ~init:empty ~f:begin fun a e ->
        match of_rest e with
        | None -> a
        | Some apikey -> add a apikey
      end
  end
end

module Feed = struct
  type t = {
    order : unit Ivar.t ;
    margin : unit Ivar.t ;
    position : unit Ivar.t ;
  }

  let create () = {
    order = Ivar.create () ;
    margin = Ivar.create () ;
    position = Ivar.create () ;
  }

  let set_order_ready { order } = Ivar.fill_if_empty order ()
  let set_margin_ready { margin } = Ivar.fill_if_empty margin ()
  let set_position_ready { position } = Ivar.fill_if_empty position ()

  let order_ready { order } = Ivar.read order
  let margin_ready { margin } = Ivar.read margin
  let position_ready { position } = Ivar.read position

  let ready { order ; margin ; position } =
    Deferred.all_unit (List.map [ order ; margin ; position ] ~f:Ivar.read)
end

module Connection = struct
  type subscribe_msg =
    | Subscribe of { addr: string; id: int }
    | Unsubscribe of { addr: string; id: int }

  type client_update = {
    userid: int;
    update: WS.Response.Update.t ;
  }

  type t = {
    addr: string;
    w: Writer.t;
    ws_r: client_update Pipe.Reader.t;
    ws_w: client_update Pipe.Writer.t;
    to_bitmex_r: subscribe_msg Pipe.Reader.t;
    to_bitmex_w: subscribe_msg Pipe.Writer.t;
    key: string;
    secret: string;
    position: Position.t String.Table.t Int.Table.t; (* indexed by account, then symbol *)
    margin: Margin.t String.Table.t Int.Table.t; (* indexed by account, then currency *)
    order: Order.t Uuid.Table.t Int.Table.t; (* indexed by account, then orderID *)
    mutable dropped: int;
    subs: int32 String.Table.t;
    rev_subs: string Int32.Table.t ;
    subs_depth: int32 String.Table.t;
    rev_subs_depth: string Int32.Table.t ;
    send_secdefs: bool;

    mutable all_apikeys : ApiKey.Set.t ;
    apikeys : ApiKey.t Int.Table.t ; (* indexed by account *)
    usernames : string Int.Table.t; (* indexed by account *)
    accounts : int String.Table.t; (* indexed by SC username *)
    subscriptions : unit Int.Table.t;
    feeds : Feed.t Int.Table.t ;
  }

  let create ~addr ~w ~key ~secret ~send_secdefs =
    let ws_r, ws_w = Pipe.create () in
    let to_bitmex_r, to_bitmex_w = Pipe.create () in
    {
      addr ;
      w ;
      ws_r ;
      ws_w ;
      to_bitmex_r ;
      to_bitmex_w ;
      key ;
      secret ;
      position = Int.Table.create () ;
      margin = Int.Table.create () ;
      order = Int.Table.create () ;
      subs = String.Table.create () ;
      rev_subs = Int32.Table.create () ;
      subs_depth = String.Table.create () ;
      rev_subs_depth = Int32.Table.create () ;
      send_secdefs ;

      all_apikeys = ApiKey.Set.empty ;
      apikeys = Int.Table.create () ;
      usernames = Int.Table.create () ;
      accounts = String.Table.create () ;
      subscriptions = Int.Table.create () ;
      feeds = Int.Table.create () ;

      dropped = 0 ;
    }

  let purge { ws_r; to_bitmex_r } =
    Pipe.close_read ws_r;
    Pipe.close_read to_bitmex_r

  let active : t String.Table.t = String.Table.create ()
  let to_alist () = String.Table.to_alist active

  let find = String.Table.find active
  let find_exn = String.Table.find_exn active
  let set = String.Table.set active
  let remove = String.Table.remove active

  let iter = String.Table.iter active

  let find_order order uuid =
    let exception Found of Order.t in try
      Int.Table.iteri order ~f:begin fun ~key ~data ->
        match Uuid.Table.find data uuid with
        | Some o -> raise (Found o)
        | None -> ()
      end;
      None
    with Found o -> Some o
end

module Books = struct
  type entry = {
    price: float;
    size: int;
  }

  let mapify_ob =
    let fold_f ~key:_ ~data:{ price; size } map =
      Float.Map.update map price ~f:begin function
        | Some size' -> size + size'
        | None -> size
      end
    in
    Int.Table.fold ~init:Float.Map.empty ~f:fold_f

  let bids : entry Int.Table.t String.Table.t = String.Table.create ()
  let asks : entry Int.Table.t String.Table.t = String.Table.create ()
  let initialized = Ivar.create ()

  let get_bids symbol =
    Option.value_map (String.Table.find bids symbol)
      ~default:Float.Map.empty ~f:mapify_ob

  let get_asks symbol =
    Option.value_map (String.Table.find asks symbol)
      ~default:Float.Map.empty ~f:mapify_ob

  let update action { OrderBookL2.symbol; id; side; size; price } =
    (* find_exn cannot raise here *)
    let bids = String.Table.find_or_add bids symbol ~default:Int.Table.create in
    let asks = String.Table.find_or_add asks symbol ~default:Int.Table.create in
    let table =
      match Side.of_string side with
      | `buy -> bids
      | `sell -> asks
      | `buy_sell_unset -> failwith "update_depth: empty side" in
    let price =
      match price with
      | Some p -> Some p
      | None -> begin match Int.Table.find table id with
          | Some { price } -> Some price
          | None -> None
        end in
    let size =
      match size with
      | Some s -> Some s
      | None -> begin match Int.Table.find table id with
          | Some { size } -> Some size
          | None -> None
        end in
    match price, size with
    | Some price, Some size ->
      begin match action with
        | Bmex_ws.Response.Update.Partial
        | Insert
        | Update -> Int.Table.set table id { size ; price }
        | Delete -> Int.Table.remove table id
      end;
      let u = DTC.default_market_depth_update_level () in
      let update_type =
        match action with
        | Partial
        | Insert
        | Update -> `market_depth_insert_update_level
        | Delete -> `market_depth_delete_level in
      let side =
        match Side.of_string side with
        | `buy -> Some `at_bid
        | `sell -> Some `at_ask
        | `buy_sell_unset -> None
      in
      u.side <- side ;
      u.price <- Some price ;
      u.quantity <- Some (Float.of_int size) ;
      u.update_type <- Some update_type ;
      let on_connection { Connection.addr; w; subs; subs_depth } =
        let on_symbol_id symbol_id =
          u.symbol_id <- Some symbol_id ;
          (* Log.debug log_dtc "-> [%s] depth %s %s %s %f %d" addr_str (OB.sexp_of_action action |> Sexp.to_string) symbol side price size; *)
          write_message w `market_depth_update_level DTC.gen_market_depth_update_level u
        in
        Option.iter String.Table.(find subs_depth symbol) ~f:on_symbol_id
      in
      Connection.iter ~f:on_connection
    | _ ->
      Log.info log_bitmex "update_depth: received update before snapshot, ignoring"
end

module Instr = struct
  let is_index symbol = symbol.[0] = '.'

  let to_secdef ~testnet (t : Instrument.t) =
    let index = is_index t.symbol in
    let exchange = (Option.value ~default:"" t.reference)
      ^ (if testnet && not index then "T" else "")
    in
    let tickSize = Option.value ~default:0. t.tickSize in
    let expiration_date = Option.map t.expiry ~f:(fun time ->
        Time_ns.(to_int_ns_since_epoch time |>
                 (fun t -> t / 1_000_000_000) |>
                 Int32.of_int_exn)) in
    let secdef = DTC.default_security_definition_response () in
    secdef.symbol <- Some t.symbol ;
    secdef.exchange <- Some exchange ;
    secdef.security_type <-
      Some (if index then `security_type_index else `security_type_future) ;
    secdef.min_price_increment <- Some tickSize ;
    secdef.currency_value_per_increment <- Some tickSize ;
    secdef.price_display_format <- Some (price_display_format_of_ticksize tickSize) ;
    secdef.has_market_depth_data <- Some (not index) ;
    secdef.underlying_symbol <- t.underlyingSymbol ;
    secdef.updates_bid_ask_only <- Some false ;
    secdef.security_expiration_date <- expiration_date ;
    secdef

  type t = {
    mutable instr: Instrument.t;
    secdef: DTC.Security_definition_response.t ;
    mutable last_trade_price: float;
    mutable last_trade_size: int;
    mutable last_trade_ts: Time_ns.t;
    mutable last_quote_ts: Time_ns.t;
  }

  let create
      ?(last_trade_price = 0.)
      ?(last_trade_size = 0)
      ?(last_trade_ts = Time_ns.epoch)
      ?(last_quote_ts = Time_ns.epoch)
      ~instr ~secdef () = {
    instr ; secdef ;
    last_trade_price ; last_trade_size ;
    last_trade_ts ; last_quote_ts
  }

  let active : t String.Table.t = String.Table.create ()
  let initialized = Ivar.create ()

  let mem = String.Table.mem active
  let find = String.Table.find active
  let find_exn = String.Table.find_exn active
  let set = String.Table.set active
  let remove = String.Table.remove active

  let iter = String.Table.iter active

  let delete t =
    remove t.Instrument.symbol ;
    Log.info log_bitmex "deleted instrument %s" t.symbol

  let insert t =
    let secdef = to_secdef ~testnet:!use_testnet t in
    let instr = create ~instr:t ~secdef () in
    set t.symbol instr;
    Log.info log_bitmex "inserted instrument %s" t.symbol;
    (* Send secdef response to connections. *)
    let on_connection { Connection.addr; w } =
      secdef.is_final_message <- Some true ;
      write_message w `security_definition_response
        DTC.gen_security_definition_response secdef
    in
    Connection.iter ~f:on_connection

  let send_instr_update_msgs w t symbol_id =
    Option.iter t.Instrument.volume ~f:begin fun volume ->
      let msg = DTC.default_market_data_update_session_volume () in
      msg.symbol_id <- Some symbol_id ;
      msg.volume <- Some Int.(to_float volume) ;
      write_message w `market_data_update_session_volume
        DTC.gen_market_data_update_session_volume msg
    end ;
    Option.iter t.lowPrice ~f:begin fun low ->
      let msg = DTC.default_market_data_update_session_low () in
      msg.symbol_id <- Some symbol_id ;
      msg.price <- Some low ;
      write_message w `market_data_update_session_low
        DTC.gen_market_data_update_session_low msg
    end ;
    Option.iter t.highPrice ~f:begin fun high ->
      let msg = DTC.default_market_data_update_session_high () in
      msg.symbol_id <- Some symbol_id ;
      msg.price <- Some high ;
      write_message w `market_data_update_session_high
        DTC.gen_market_data_update_session_high msg
    end ;
    Option.iter t.openInterest ~f:begin fun open_interest ->
      let msg = DTC.default_market_data_update_open_interest () in
      msg.symbol_id <- Some symbol_id ;
      msg.open_interest <- Some (Int.to_int32_exn open_interest) ;
      write_message w `market_data_update_open_interest
        DTC.gen_market_data_update_open_interest msg
    end ;
    Option.iter t.prevClosePrice ~f:begin fun prev_close ->
      let msg = DTC.default_market_data_update_session_open () in
      msg.symbol_id <- Some symbol_id ;
      msg.price <- Some prev_close ;
      write_message w `market_data_update_session_open
        DTC.gen_market_data_update_session_open msg
    end

  let update t =
    match find t.Instrument.symbol with
    | None ->
      Log.error log_bitmex "update_instr: unable to find %s" t.symbol;
    | Some instr ->
      instr.instr <- Instrument.merge instr.instr t;
      Log.debug log_bitmex "updated instrument %s" t.symbol;
      (* Send messages to subscribed clients according to the type of update. *)
      let on_connection { Connection.addr; w; subs } =
        let on_symbol_id symbol_id =
          send_instr_update_msgs w t symbol_id;
          Log.debug log_dtc "-> [%s] instrument %s" addr t.symbol
        in
        Option.iter String.Table.(find subs t.symbol) ~f:on_symbol_id
      in
      Connection.iter ~f:on_connection
end

module Quotes = struct
  let quotes : Quote.t String.Table.t = String.Table.create ()
  let initialized = Ivar.create ()

  let find = String.Table.find quotes
  let find_exn = String.Table.find_exn quotes

  let update ({ Quote.timestamp; symbol; bidPrice; bidSize; askPrice; askSize } as q) =
    let old_q = String.Table.find_or_add quotes symbol ~default:(fun () -> q) in
    let merged_q = Quote.merge old_q q in
    let bidPrice = Option.value ~default:Float.max_finite_value merged_q.bidPrice in
    let bidSize = Option.value ~default:0 merged_q.bidSize in
    let askPrice = Option.value ~default:Float.max_finite_value merged_q.askPrice in
    let askSize = Option.value ~default:0 merged_q.askSize in
    String.Table.set quotes ~key:q.symbol ~data:merged_q;
    Log.debug log_bitmex "set quote %s" q.symbol;
    let u = DTC.default_market_data_update_bid_ask () in
    u.bid_price <- Some bidPrice ;
    u.bid_quantity <- Some (Float.of_int bidSize) ;
    u.ask_price <- Some askPrice ;
    u.ask_quantity <- Some (Float.of_int askSize) ;
    u.date_time <- Some (seconds_int32_of_ts merged_q.timestamp) ;
    let on_connection { Connection.addr; w; subs; subs_depth } =
      let on_symbol_id symbol_id =
        u.symbol_id <- Some symbol_id ;
        Log.debug log_dtc "-> [%s] bidask %s %f %d %f %d"
          addr q.symbol bidPrice bidSize askPrice askSize ;
        write_message w `market_data_update_bid_ask
          DTC.gen_market_data_update_bid_ask u
      in
      match String.Table.(find subs q.symbol, find subs_depth q.symbol) with
      | Some id, None -> on_symbol_id id
      | _ -> ()
    in
    Connection.iter ~f:on_connection
end

module TradeHistory = struct
  type trade = {
    e : Execution.t ;
    trade_account : string ;
  }

  let create_trade ~userid ~username ~e =
    { trade_account = trade_accountf ~userid ~username; e }

  let buf = Bi_outbuf.create 4096
  let trades_by_uuid : trade Uuid.Table.t = Uuid.Table.create ()
  let table : trade Uuid.Map.t Int.Table.t = Int.Table.create ()

  let get ~userid ~username ~key ~secret =
    let filter = `Assoc ["execType", `String "Trade"] in
    REST.Execution.trade_history ~count:500
      ~buf ~log:log_bitmex ~testnet:!use_testnet
      ~filter ~key ~secret () >>| function
    | Error err ->
      Log.error log_bitmex "%s" (Error.to_string_hum err) ;
      Error err
    | Ok (resp, trades) ->
      let trades =
        List.fold_left trades ~init:Uuid.Map.empty ~f:begin fun a e ->
          match e.orderID with
          | None -> a
          | Some uuid ->
              let trade = create_trade ~userid ~username ~e in
              Uuid.Table.set trades_by_uuid uuid trade ;
              Uuid.Map.set a uuid trade
        end in
      Int.Table.set table userid trades ;
      Ok trades

  let filter_trades min_ts trades =
    Uuid.Map.filter trades ~f:begin fun { e } ->
      let ts =
        Option.value e.transactTime ~default:Time_ns.max_value  in
      Time_ns.(ts > min_ts)
    end

  let get ?(min_ts=Time_ns.epoch) { Connection.apikeys ; usernames } ~userid =
    let { ApiKey.id = key ; secret } = Int.Table.find_exn apikeys userid in
    let username = Int.Table.find_exn usernames userid in
    match Int.Table.find table userid with
    | Some trades -> Deferred.Or_error.return (filter_trades min_ts trades)
    | None ->
        Deferred.Or_error.(get ~userid ~username ~key ~secret >>| filter_trades min_ts)

  let get_one = Uuid.Table.find trades_by_uuid

  let set ~userid ~username e =
    let trade = create_trade ~userid ~username ~e in
    Option.iter e.orderID ~f:begin fun uuid ->
      Uuid.Table.set trades_by_uuid uuid trade ;
      let data = Option.value_map (Int.Table.find table userid)
          ~default:(Uuid.Map.singleton uuid trade)
          ~f:(Uuid.Map.set ~key:uuid ~data:trade) in
      Int.Table.set table ~key:userid ~data
    end
end

let send_heartbeat { Connection.addr ; w } span =
  let msg = DTC.default_heartbeat () in
  Clock_ns.every
    ~stop:(Writer.close_started w)
    ~continue_on_error:false span
    begin fun () ->
      (* Log.debug log_dtc "-> [%s] HB" addr ; *)
      write_message w `heartbeat DTC.gen_heartbeat msg
    end

let write_exec_update ?request_id ~nb_msgs ~msg_number ~status ~reason ~userid ~username w (e : Execution.t) =
  let cumQty = Option.map e.cumQty ~f:Int.to_float in
  let leavesQty = Option.map e.leavesQty ~f:Int.to_float in
  let orderQty = Option.map2 cumQty leavesQty ~f:Float.add in
  let u = DTC.default_order_update () in
  let price = Option.value ~default:Float.max_finite_value e.price in
  let stopPx = Option.value ~default:Float.max_finite_value e.stopPx in
  let side = Option.map e.side ~f:Side.of_string in
  let ordType = Option.map e.ordType ~f:OrderType.of_string in
  let timeInForce = Option.map e.timeInForce ~f:TimeInForce.of_string in
  let ts = Option.map e.transactTime ~f:seconds_int64_of_ts in
  let p1, p2 = OrderType.to_p1_p2 ~stopPx ~price
      (Option.value ~default:`order_type_unset ordType) in
  u.request_id <- request_id ;
  u.total_num_messages <- Some (Int32.of_int_exn nb_msgs) ;
  u.message_number <- Some (Int32.of_int_exn msg_number) ;
  u.symbol <- e.symbol ;
  u.exchange <- Some !my_exchange ;
  u.client_order_id <- e.clOrdID ;
  u.server_order_id <- Option.map ~f:Uuid.to_string e.orderID ;
  u.exchange_order_id <- Option.map ~f:Uuid.to_string e.orderID ;
  u.order_type <- ordType ;
  u.order_status <- Some status ;
  u.order_update_reason <- Some reason ;
  u.buy_sell <- side ;
  u.price1 <- p1 ;
  u.price2 <- p2 ;
  u.time_in_force <- timeInForce ;
  u.order_quantity <- orderQty ;
  u.filled_quantity <- cumQty ;
  u.remaining_quantity <- leavesQty ;
  u.average_fill_price <- e.avgPx ;
  u.last_fill_price <- e.lastPx ;
  u.last_fill_date_time <- ts ;
  u.last_fill_quantity <- Option.map ~f:Int.to_float e.lastQty ;
  u.last_fill_execution_id <- Some (Uuid.to_string e.execID) ;
  u.trade_account <- Some (trade_accountf ~userid ~username) ;
  u.info_text <- e.ordRejReason ;
  u.free_form_text <- e.text ;
  write_message w `order_update DTC.gen_order_update u

let write_order_update ?request_id ~nb_msgs ~msg_number ~status ~reason ~userid ~username w (e : Order.t) =
  let cumQty = Option.map e.cumQty ~f:Int.to_float in
  let leavesQty = Option.map e.leavesQty ~f:Int.to_float in
  let orderQty = Option.map2 cumQty leavesQty ~f:Float.add in
  let u = DTC.default_order_update () in
  let price = Option.value ~default:Float.max_finite_value e.price in
  let stopPx = Option.value ~default:Float.max_finite_value e.stopPx in
  let side = Option.map e.side ~f:Side.of_string in
  let ordType = Option.map e.ordType ~f:OrderType.of_string in
  let timeInForce = Option.map e.timeInForce ~f:TimeInForce.of_string in
  let ts = Option.map e.transactTime ~f:seconds_int64_of_ts in
  let p1, p2 = OrderType.to_p1_p2 ~stopPx ~price
      (Option.value ~default:`order_type_unset ordType) in
  u.request_id <- request_id ;
  u.total_num_messages <- Some (Int32.of_int_exn nb_msgs) ;
  u.message_number <- Some (Int32.of_int_exn msg_number) ;
  u.symbol <- e.symbol ;
  u.exchange <- Some !my_exchange ;
  u.client_order_id <- e.clOrdID ;
  u.server_order_id <- Some (Uuid.to_string e.orderID) ;
  u.exchange_order_id <- Some (Uuid.to_string e.orderID) ;
  u.order_type <- ordType ;
  u.order_status <- Some status ;
  u.order_update_reason <- Some reason ;
  u.buy_sell <- side ;
  u.price1 <- p1 ;
  u.price2 <- p2 ;
  u.time_in_force <- timeInForce ;
  u.order_quantity <- orderQty ;
  u.filled_quantity <- cumQty ;
  u.remaining_quantity <- leavesQty ;
  u.average_fill_price <- e.avgPx ;
  u.last_fill_date_time <- ts ;
  u.trade_account <- Some (trade_accountf ~userid ~username) ;
  u.info_text <- e.ordRejReason ;
  u.free_form_text <- e.text ;
  write_message w `order_update DTC.gen_order_update u

let fail_ordStatus_execType ~ordStatus ~execType =
  invalid_argf
    "Wrong ordStatus/execType pair: %s, %s"
    (OrdStatus.show ordStatus)
    (ExecType.show execType)
    ()

let fail_ordStatus ordStatus =
  invalid_argf "Wrong ordStatus: %s" (OrdStatus.show ordStatus) ()

let status_reason_of_execType_ordStatus execType ordStatus =
  match ordStatus, execType with

  | OrdStatus.New, ExecType.New
  | New, TriggeredOrActivatedBySystem -> `order_status_open, `new_order_accepted
  | New, Replaced -> `order_status_open, `order_cancel_replace_complete
  | New, Restated -> `order_status_open, `general_order_update

  | PartiallyFilled, Trade -> `order_status_partially_filled, `order_filled_partially
  | PartiallyFilled, Replaced -> `order_status_partially_filled, `order_cancel_replace_complete
  | PartiallyFilled, Restated -> `order_status_partially_filled, `general_order_update

  | Filled, Trade -> `order_status_filled, `order_filled
  | Canceled, Canceled -> `order_status_canceled, `order_canceled
  | Rejected, Rejected -> `order_status_rejected, `new_order_rejected

  | _, Funding -> raise Exit
  | _, Settlement -> raise Exit
  | _ -> fail_ordStatus_execType ~ordStatus ~execType

let status_reason_of_ordStatus = function
  | OrdStatus.New -> `order_status_open, `new_order_accepted
  | PartiallyFilled -> `order_status_partially_filled, `order_filled_partially
  | Filled -> `order_status_filled, `order_filled
  | Canceled -> `order_status_canceled, `order_canceled
  | Rejected -> `order_status_rejected, `new_order_rejected
  | ordStatus -> fail_ordStatus ordStatus

let write_exec_update ?request_id ?(nb_msgs=1) ?(msg_number=1) ?status_reason ~userid ~username w e =
  match status_reason with
  | Some (status, reason) ->
      write_exec_update ?request_id ~nb_msgs ~msg_number ~status ~reason ~userid ~username w e
  | None ->
      let ordStatus =
        Option.value_map e.ordStatus ~default:(OrdStatus.Unknown "") ~f:OrdStatus.of_string in
      let execType =
        Option.value_map e.execType ~default:(ExecType.Unknown "") ~f:ExecType.of_string in
      match status_reason_of_execType_ordStatus execType ordStatus with
      | exception Exit -> ()
      | exception Invalid_argument msg ->
          Log.error log_bitmex "Not sending order update for %s" msg ;
          ()
      | status, reason ->
          write_exec_update ?request_id ~nb_msgs ~msg_number ~status ~reason ~userid ~username w e

let write_order_update ?request_id ?(nb_msgs=1) ?(msg_number=1) ?status_reason ~userid ~username w o =
  match status_reason with
  | Some (status, reason) ->
      write_order_update ?request_id ~nb_msgs ~msg_number ~status ~reason ~userid ~username w o
  | None ->
      let ordStatus =
        Option.value_map o.ordStatus ~default:(OrdStatus.Unknown "") ~f:OrdStatus.of_string in
      match status_reason_of_ordStatus ordStatus with
      | exception Exit -> ()
      | exception Invalid_argument msg ->
          Log.error log_bitmex "Not sending order update for %s" msg ;
          ()
      | status, reason ->
          write_order_update ?request_id ~nb_msgs ~msg_number ~status ~reason ~userid ~username w o

let write_position_update ?request_id ?(nb_msgs=1) ?(msg_number=1) ~userid ~username w (p : Position.t) =
  let u = DTC.default_position_update () in
  u.total_number_messages <- Some (Int32.of_int_exn nb_msgs) ;
  u.message_number <- Some (Int32.of_int_exn msg_number) ;
  u.request_id <- request_id ;
  u.symbol <- Some p.symbol ;
  u.exchange <- Some !my_exchange ;
  u.trade_account <- Some (trade_accountf ~userid ~username) ;
  u.average_price <- p.avgEntryPrice ;
  u.quantity <- Option.map p.currentQty ~f:Int.to_float ;
  write_message w `position_update DTC.gen_position_update u

let write_balance_update ?request_id ?(msg_number=1) ?(nb_msgs=1) ~userid ~username w (m : Margin.t) =
  let margin_req = match m.initMargin, m.maintMargin, m.sessionMargin with
  | Some a, Some b, Some c -> Some ((a+b+c) // 100000)
  | _ -> None in
  let u = DTC.default_account_balance_update () in
  u.request_id <- request_id ;
  u.unsolicited <- Some (Option.is_none request_id) ;
  u.total_number_messages <- Some (Int32.of_int_exn nb_msgs) ;
  u.message_number <- Some (Int32.of_int_exn msg_number) ;
  u.account_currency <- Some "mXBT" ;
  u.cash_balance <- Option.map m.walletBalance ~f:(fun b -> b // 100000) ;
  u.balance_available_for_new_positions <-
    Option.map m.availableMargin ~f:(fun b -> b // 100000) ;
  u.securities_value <- Option.map m.marginBalance ~f:(fun b -> b // 100000) ;
  u.margin_requirement <- margin_req ;
  u.trade_account <- Some (trade_accountf ~userid ~username) ;
  write_message w `account_balance_update DTC.gen_account_balance_update u

let write_trade_account ?request_id ~message_number ~total_number_messages ~trade_account w =
  let resp = DTC.default_trade_account_response () in
  resp.total_number_messages <- Some (Int32.of_int_exn total_number_messages) ;
  resp.message_number <- Some (Int32.of_int_exn message_number) ;
  resp.trade_account <- Some trade_account ;
  resp.request_id <- request_id ;
  write_message w `trade_account_response DTC.gen_trade_account_response resp

let write_trade_accounts ?request_id
    { Connection.addr ; w ; usernames } =
  let total_number_messages, trade_accounts =
    Int.Table.fold usernames ~init:(0, [])
      ~f:begin fun ~key:userid ~data:username (i, accounts) ->
        succ i, trade_accountf ~userid ~username :: accounts
      end in
  List.iteri trade_accounts ~f:begin fun i trade_account ->
    write_trade_account ?request_id
      ~message_number:(succ i) ~total_number_messages ~trade_account w
  end ;
  Log.debug log_dtc "-> [%s] Trade Account Response: %d accounts"
    addr total_number_messages

let add_api_keys
    { Connection.addr; apikeys; usernames; accounts }
    ({ ApiKey.userId ; username } as apikey) =
  Int.Table.set apikeys ~key:userId ~data:apikey ;
  Int.Table.set usernames userId username ;
  String.Table.set accounts username userId ;
  Log.debug log_bitmex "[%s] Found account %s:%d" addr username userId

let rec populate_api_keys
    ({ Connection.addr ; apikeys ; usernames ; all_apikeys ;
       order ; position ; margin ;
       accounts ; feeds ; to_bitmex_w } as conn) entries =
  Log.debug log_bitmex "[%s] Found %d api keys entries"
    addr (List.length entries) ;
  let new_apikeys = ApiKey.Set.of_rest entries in
  let keys_to_add = ApiKey.Set.diff new_apikeys all_apikeys in
  let keys_to_delete = ApiKey.Set.diff all_apikeys new_apikeys in
  conn.all_apikeys <- new_apikeys ;
  Int.Table.clear apikeys;
  Int.Table.clear usernames;
  String.Table.clear accounts;
  ApiKey.Set.iter new_apikeys ~f:(add_api_keys conn) ;
  if not ApiKey.Set.(is_empty keys_to_delete && is_empty keys_to_add) then
    write_trade_accounts conn ;
  Log.debug log_bitmex "[%s] add %d key(s), delete %d key(s)" addr
    (ApiKey.Set.length keys_to_add)
    (ApiKey.Set.length keys_to_delete);
  Deferred.List.iter
    (ApiKey.Set.to_list keys_to_delete) ~how:`Sequential ~f:begin fun { userId } ->
    Log.debug log_bitmex "[%s] Unsubscribe %d" addr userId ;
    Int.Table.remove order userId ;
    Int.Table.remove position userId ;
    Int.Table.remove margin userId ;
    Int.Table.remove feeds userId ;
    Pipe.write to_bitmex_w @@ Unsubscribe { addr ; id = userId }
  end >>= fun () ->
  Deferred.List.iter
    (ApiKey.Set.to_list keys_to_add) ~how:`Sequential ~f:begin fun { userId } ->
    Log.debug log_bitmex "[%s] Subscribe %d" addr userId;
    Int.Table.set order userId (Uuid.Table.create ()) ;
    Int.Table.set position userId (String.Table.create ()) ;
    Int.Table.set margin userId (String.Table.create ()) ;
    Int.Table.set feeds userId (Feed.create ()) ;
    Pipe.write to_bitmex_w @@ Subscribe { addr ; id = userId }
  end

let populate_api_keys ({ Connection.key ; secret } as conn) =
  REST.ApiKey.dtc ~log:log_bitmex ~key ~secret ~testnet:!use_testnet () |>
  Deferred.Or_error.bind ~f:begin fun (_response, entries) ->
    Monitor.try_with_or_error (fun () -> populate_api_keys conn entries)
  end

let process_order { Connection.addr ; w ; order } partial_iv action (o : Order.t) =
  match action, Option.(o.account >>| Int.Table.find_or_add order ~default:Uuid.Table.create) with
  | _, None -> ()
  | WS.Response.Update.Delete, Some table ->
      Uuid.Table.remove table o.orderID;
      Log.debug log_bitmex "<- [%s] order delete" addr
  | Insert, Some table
  | Partial, Some table ->
      Uuid.Table.set table ~key:o.orderID ~data:o;
  | Update, Some table ->
      if Ivar.is_full partial_iv then begin
        let data = match Uuid.Table.find table o.orderID with
        | None -> o
        | Some old_o -> Order.merge old_o o
        in
        Uuid.Table.set table ~key:o.orderID ~data;
      end

let process_margins { Connection.addr ; w ; margin ; usernames } partial_iv action i (m : Margin.t) =
  let username = Int.Table.find_exn usernames m.account in
  let margin = Int.Table.find_or_add ~default:String.Table.create margin m.account in
  match action with
  | WS.Response.Update.Delete ->
      String.Table.remove margin m.currency ;
      Log.debug log_bitmex "<- [%s] margin delete" addr
  | Insert | Partial ->
      String.Table.set margin ~key:m.currency ~data:m ;
      write_balance_update ~username ~userid:m.account w m ;
      Log.debug log_bitmex "-> [%s] margin (%s:%d)" addr username m.account
  | Update ->
      if Ivar.is_full partial_iv then begin
        let m = match String.Table.find margin m.currency with
        | None -> m
        | Some old_m -> Margin.merge old_m m
        in
        String.Table.set margin ~key:m.currency ~data:m;
        write_balance_update ~username ~userid:m.account w m ;
        Log.debug log_bitmex "-> [%s] margin (%s:%d)" addr username m.account
      end

let process_positions { Connection.addr ; w ; position ; usernames } partial_iv action (p : Position.t) =
  let username = Int.Table.find_exn usernames p.account in
  let position = Int.Table.find_or_add ~default:String.Table.create position p.account in
  match action with
  | WS.Response.Update.Delete ->
      String.Table.remove position p.symbol ;
      Log.debug log_bitmex "<- [%s] position delete" addr
  | Insert | Partial ->
      String.Table.set position ~key:p.symbol ~data:p;
      if Option.value ~default:false p.isOpen then begin
        write_position_update ~userid:p.account ~username w p;
        Log.debug log_bitmex
          "-> [%s] position %s (%s:%d)" addr p.symbol username p.account
      end
  | Update ->
      if Ivar.is_full partial_iv then begin
        let old_p, p = match String.Table.find position p.symbol with
        | None -> None, p
        | Some old_p -> Some old_p, Position.merge old_p p
        in
        String.Table.set position ~key:p.symbol ~data:p;
        match old_p with
        | Some old_p when Option.value ~default:false old_p.isOpen ->
            write_position_update ~userid:p.account ~username w p ;
            Log.debug log_bitmex
              "-> [%s] position %s (%s:%d)" addr p.symbol username p.account
        | _ -> ()
      end

let process_execs { Connection.addr ; w ; usernames } (e : Execution.t) =
  Option.iter e.account ~f:begin fun account ->
    let username = Int.Table.find_exn usernames account in
    let execType = Option.map e.execType ~f:ExecType.of_string in
    let ordStatus = Option.map e.ordStatus ~f:OrdStatus.of_string in
    begin match execType, ordStatus with
    | Some Trade, Some Filled -> TradeHistory.set ~userid:account ~username e
    | _ -> ()
    end ;
    write_exec_update ~userid:account ~username w e
  end

let on_client_ws_update
    c { Connection.userid ; update={ WS.Response.Update.table; action; data } } =
  let feed =
    Int.Table.find_or_add c.Connection.feeds userid ~default:Feed.create in
  match table, action, data with
  | Order, action, orders ->
      List.iter orders ~f:(Fn.compose (process_order c feed.order action) Order.of_yojson) ;
      if action = Partial then Ivar.fill_if_empty feed.order ()
  | Margin, action, margins ->
      List.iteri margins ~f:(fun i m -> process_margins c feed.margin action i (Margin.of_yojson m)) ;
      if action = Partial then Ivar.fill_if_empty feed.margin ()
  | Position, action, positions ->
      List.iter positions ~f:(Fn.compose (process_positions c feed.position action) Position.of_yojson) ;
      if action = Partial then Ivar.fill_if_empty feed.position ()
  | Execution, action, execs ->
      List.iter execs ~f:(Fn.compose (process_execs c) Execution.of_yojson)
  | table, _, _ ->
      Log.error log_bitmex "Unknown table %s" (Bmex_ws.Topic.to_string table)

let client_ws ({ Connection.addr; w; ws_r; key; secret; order; margin; position } as c) =
  let start = populate_api_keys c in
  Clock_ns.every
    ~continue_on_error:true
    ~start:(Time_ns.(Clock_ns.after (Time_ns.Span.of_int_sec 60)))
    ~stop:(Writer.close_started w)
    Time_ns.Span.(of_int_sec 60)
    begin fun () -> don't_wait_for begin
        populate_api_keys c >>| function
        | Ok () -> ()
        | Error err -> Log.error log_bitmex "%s" @@ Error.to_string_hum err
      end
    end ;
  don't_wait_for @@ Monitor.handle_errors
    (fun () ->
       Pipe.iter_without_pushback ~continue_on_error:true ws_r ~f:(on_client_ws_update c))
    (fun exn ->
       Log.error log_bitmex "%s" @@ Exn.to_string exn);
  start

let new_client_accepted, new_client_accepted_w = Pipe.create ()
let client_deleted, client_deleted_w = Pipe.create ()

let encoding_request addr w req =
  Log.debug log_dtc "<- [%s] Encoding Request" addr ;
  Dtc_pb.Encoding.(to_string (Response { version = 7 ; encoding = Protobuf })) |>
  Writer.write w ;
  Log.debug log_dtc "-> [%s] Encoding Response" addr

let accept_logon_request addr w req conn send_secdefs trading_supported =
  let hb_span =
    Option.value_map req.DTC.Logon_request.heartbeat_interval_in_seconds
      ~default:(Time_ns.Span.of_int_sec 10)
      ~f:(fun span -> Time_ns.Span.of_int_sec (Int32.to_int_exn span)) in
  let result_text =
    if trading_supported then
      "Welcome to BitMEX DTC Server for Sierra Chart"
    else
    "Welcome to BitMEX DTC Server for Sierra Chart (data only)" in
  let r = DTC.default_logon_response () in
  r.protocol_version <- Some 7l ;
  r.server_name <- Some "BitMEX" ;
  r.result <- Some `logon_success ;
  r.result_text <- Some result_text ;
  r.symbol_exchange_delimiter <- Some "-" ;
  r.security_definitions_supported <- Some true ;
  r.market_data_supported <- Some true ;
  r.historical_price_data_supported <- Some false ;
  r.market_depth_is_supported <- Some true ;
  r.market_depth_updates_best_bid_and_ask <- Some true ;
  r.trading_is_supported <- Some trading_supported ;
  r.order_cancel_replace_supported <- Some trading_supported ;
  r.ocoorders_supported <- Some false ;
  r.bracket_orders_supported <- Some false ;

  send_heartbeat conn hb_span ;
  write_message w `logon_response DTC.gen_logon_response r ;

  Log.debug log_dtc "-> [%s] Logon Response" addr ;
  let on_instrument { Instr.secdef } =
    secdef.request_id <- Some 110_000_000l ;
    secdef.is_final_message <- Some true ;
    write_message w `security_definition_response
      DTC.gen_security_definition_response secdef
  in
  if send_secdefs then Instr.iter ~f:on_instrument

let reject_logon_request addr w k =
  let r = DTC.default_logon_response () in
  r.protocol_version <- Some 7l ;
  r.server_name <- Some "BitMEX" ;
  r.result <- Some `logon_error ;
  Printf.ksprintf begin fun result_text ->
    r.result_text <- Some result_text ;
    write_message w `logon_response DTC.gen_logon_response r
  end k

let logon_request addr w msg =
  Log.debug log_dtc "<- [%s] Logon Request" addr ;
  let req = DTC.parse_logon_request msg in
  let int1 = Option.value ~default:0l req.integer_1 in
  let send_secdefs = Int32.(bit_and int1 128l <> 0l) in
  let username = Option.value ~default:"" req.username in
  let password = Option.value ~default:"" req.password in
  match username, password with
  | key, secret when key <> "" ->
      let conn = Connection.create ~addr ~w ~key ~secret ~send_secdefs in
      Connection.set ~key:addr ~data:conn;
      don't_wait_for begin
        Pipe.write new_client_accepted_w conn >>= fun () ->
        client_ws conn >>| function
        | Ok () -> accept_logon_request addr w req conn send_secdefs true
        | Error err ->
            Log.error log_bitmex "%s" @@ Error.to_string_hum err ;
            reject_logon_request addr w "Credentials rejected by BitMEX"
      end
  | _ ->
      let conn = Connection.create ~addr ~w ~key:"" ~secret:"" ~send_secdefs in
      Connection.set ~key:addr ~data:conn ;
      accept_logon_request addr w req conn send_secdefs false

let heartbeat addr w msg =
  (* Log.debug log_dtc "<- [%s] Heartbeat" addr *)
  ()

let security_definition_reject addr w request_id k =
  Printf.ksprintf begin fun msg ->
    let resp = DTC.default_security_definition_reject () in
    resp.request_id <- Some request_id ;
    resp.reject_text <- Some msg ;
    Log.debug log_dtc "-> [%s] Security Definition Reject" addr ;
    write_message w `security_definition_reject DTC.gen_security_definition_reject resp
  end k

let security_definition_request addr w msg =
  let req = DTC.parse_security_definition_for_symbol_request msg in
  match req.request_id, req.symbol, req.exchange with
  | Some id, Some symbol, Some exchange ->
    Log.debug log_dtc "<- [%s] Security Definition Request %s %s" addr symbol exchange;
    if !my_exchange <> exchange && not Instr.(is_index symbol) then
      security_definition_reject addr w id "No such symbol %s %s" symbol exchange
    else begin
      match Instr.find symbol with
      | None ->
        security_definition_reject addr w id "No such symbol %s %s" symbol exchange
      | Some { secdef } ->
        secdef.request_id <- Some id ;
        secdef.is_final_message <- Some true ;
        Log.debug log_dtc
          "-> [%s] Security Definition Response %s %s" addr symbol exchange;
        write_message w `security_definition_response
          DTC.gen_security_definition_response secdef
    end
  | _ ->
    Log.error log_dtc "<- [%s] BAD Security Definition Request" addr

let reject_market_data_request ?id addr w k =
  Printf.ksprintf begin fun reason ->
    let resp = DTC.default_market_data_reject () in
    resp.symbol_id <- id ;
    resp.reject_text <- Some reason ;
    Log.debug log_dtc "-> [%s] Market Data Reject" addr ;
    write_message w `market_data_reject DTC.gen_market_data_reject resp
  end k

let write_market_data_snapshot ?id addr w symbol
    { Instr.instr; last_trade_price;
      last_trade_size; last_trade_ts; last_quote_ts } =
  if Instr.is_index symbol then begin
    let snap = DTC.default_market_data_snapshot () in
    snap.symbol_id <- id ;
    snap.session_settlement_price <- instr.prevPrice24h ;
    snap.last_trade_price <- instr.lastPrice ;
    snap.last_trade_date_time <- Option.map instr.timestamp ~f:seconds_float_of_ts ;
    write_message w `market_data_snapshot DTC.gen_market_data_snapshot snap
  end
  else begin
    let { Quote.bidPrice; bidSize; askPrice; askSize } = Quotes.find_exn symbol in
    let open Option in
    let snap = DTC.default_market_data_snapshot () in
    snap.session_settlement_price <-
      Some (value ~default:Float.max_finite_value instr.indicativeSettlePrice) ;
    snap.session_high_price <-
      Some (value ~default:Float.max_finite_value instr.highPrice) ;
    snap.session_low_price <-
      Some (value ~default:Float.max_finite_value instr.lowPrice) ;
    snap.session_volume <-
      Some (value_map instr.volume ~default:Float.max_finite_value ~f:Int.to_float) ;
    snap.open_interest <-
      Some (value_map instr.openInterest ~default:0xffffffffl ~f:Int.to_int32_exn) ;
    snap.bid_price <- bidPrice ;
    snap.bid_quantity <- Option.(map bidSize ~f:Float.of_int) ;
    snap.ask_price <- askPrice ;
    snap.ask_quantity <- Option.(map askSize ~f:Float.of_int) ;
    snap.last_trade_price <- Some last_trade_price ;
    snap.last_trade_volume <- Some (Int.to_float last_trade_size) ;
    snap.last_trade_date_time <- Some (seconds_float_of_ts last_trade_ts) ;
    snap.bid_ask_date_time <- Some (seconds_float_of_ts last_quote_ts) ;
    write_message w `market_data_snapshot DTC.gen_market_data_snapshot snap
  end

let market_data_request addr w msg =
  let req = DTC.parse_market_data_request msg in
  let { Connection.subs ; rev_subs } = Connection.find_exn addr in
  match req.request_action,
        req.symbol_id,
        req.symbol,
        req.exchange
  with
  | _, id, Some symbol, Some exchange
    when exchange <> !my_exchange && Instr.(is_index symbol) ->
    reject_market_data_request ?id addr w "No such exchange %s" exchange
  | _, id, Some symbol, _ when not (Instr.mem symbol) ->
    reject_market_data_request ?id addr w "No such symbol %s" symbol
  | Some `unsubscribe, Some id, _, _ ->
    begin match Int32.Table.find rev_subs id with
    | None -> ()
    | Some symbol -> String.Table.remove subs symbol
    end ;
    Int32.Table.remove rev_subs id
  | Some `snapshot, id, Some symbol, Some exchange ->
    Log.debug log_dtc "<- [%s] Market Data Request (snapshot) %s %s"
      addr symbol exchange ;
    let instr = Instr.find_exn symbol in
    write_market_data_snapshot ?id addr w symbol instr ;
    Log.debug log_dtc "-> [%s] Market Data Snapshot %s %s" addr symbol exchange
  | Some `subscribe, Some id, Some symbol, Some exchange ->
    Log.debug log_dtc "<- [%s] Market Data Request (subscribe) %ld %s %s"
      addr id symbol exchange ;
    begin
      match Int32.Table.find rev_subs id with
      | Some symbol' when symbol <> symbol' ->
        reject_market_data_request addr w ~id
          "Already subscribed to %s %s with a different id (was %ld)"
          symbol exchange id
      | _ ->
        String.Table.set subs symbol id ;
        Int32.Table.set rev_subs id symbol ;
        let instr = Instr.find_exn symbol in
        write_market_data_snapshot ~id addr w symbol instr ;
        Log.debug log_dtc "-> [%s] Market Data Snapshot %s %s" addr symbol exchange
    end
  | _ ->
    reject_market_data_request addr w "Market Data Request: wrong request"

let reject_market_depth_request ?id addr w k =
  Printf.ksprintf begin fun reject_text ->
    let rej = DTC.default_market_depth_reject () in
    rej.symbol_id <- id ;
    rej.reject_text <- Some reject_text ;
    Log.debug log_dtc "-> [%s] Market Depth Reject: %s" addr reject_text;
    write_message w `market_depth_reject
      DTC.gen_market_depth_reject rej
  end k

let write_market_depth_snapshot ?id addr w ~symbol ~num_levels =
  let bids = Books.get_bids symbol in
  let asks = Books.get_asks symbol in
  let snap = DTC.default_market_depth_snapshot_level () in
  snap.symbol_id <- id ;
  snap.side <- Some `at_bid ;
  snap.is_last_message_in_batch <- Some false ;
  Float.Map.fold_right bids ~init:1 ~f:begin fun ~key:price ~data:size lvl ->
    snap.price <- Some price ;
    snap.quantity <- Some (Float.of_int size) ;
    snap.level <- Some (Int32.of_int_exn lvl) ;
    snap.is_first_message_in_batch <- Some (lvl = 1) ;
    write_message w `market_depth_snapshot_level DTC.gen_market_depth_snapshot_level snap ;
    succ lvl
  end |> ignore;
  snap.side <- Some `at_ask ;
  Float.Map.fold asks ~init:1 ~f:begin fun ~key:price ~data:size lvl ->
    snap.price <- Some price ;
    snap.quantity <- Some (Float.of_int size) ;
    snap.level <- Some (Int32.of_int_exn lvl) ;
    snap.is_first_message_in_batch <- Some (lvl = 1 && Float.Map.is_empty bids) ;
    write_message w `market_depth_snapshot_level DTC.gen_market_depth_snapshot_level snap ;
    succ lvl
  end |> ignore;
  snap.price <- None ;
  snap.quantity <- None ;
  snap.level <- None ;
  snap.is_first_message_in_batch <- Some false ;
  snap.is_last_message_in_batch <- Some true ;
  write_message w `market_depth_snapshot_level DTC.gen_market_depth_snapshot_level snap

let market_depth_request addr w msg =
  let req = DTC.parse_market_depth_request msg in
  let num_levels = Option.value_map req.num_levels ~default:50 ~f:Int32.to_int_exn in
  let { Connection.subs_depth ; rev_subs_depth } = Connection.find_exn addr in
  match req.request_action,
        req.symbol_id,
        req.symbol,
        req.exchange
  with
  | _, id, _, Some exchange when exchange <> !my_exchange ->
    reject_market_depth_request ?id addr w "No such exchange %s" exchange
  | _, id, Some symbol, _ when not (Instr.mem symbol) ->
    reject_market_data_request ?id addr w "No such symbol %s" symbol
  | Some `unsubscribe, Some id, _, _ ->
    begin match Int32.Table.find rev_subs_depth id with
    | None -> ()
    | Some symbol -> String.Table.remove subs_depth symbol
    end ;
    Int32.Table.remove rev_subs_depth id
  | Some `snapshot, id, Some symbol, Some exchange ->
    write_market_depth_snapshot ?id addr w ~symbol ~num_levels
  | Some `subscribe, Some id, Some symbol, Some exchange ->
    Log.debug log_dtc "<- [%s] Market Data Request %ld %s %s"
      addr id symbol exchange ;
    begin
      match Int32.Table.find rev_subs_depth id with
      | Some symbol' when symbol <> symbol' ->
        reject_market_data_request addr w ~id
          "Already subscribed to %s %s with a different id (was %ld)"
          symbol exchange id
      | _ ->
        String.Table.set subs_depth symbol id ;
        Int32.Table.set rev_subs_depth id symbol ;
        write_market_depth_snapshot ~id addr w ~symbol ~num_levels
    end
  | _ ->
      reject_market_data_request addr w "Market Data Request: wrong request"

let order_status_if_open o : DTC.order_status_enum option =
  let open Option.Monad_infix in
  o.Order.ordStatus >>|
  OrdStatus.of_string >>= function
  | New -> Some `order_status_open
  | PartiallyFilled -> Some `order_status_partially_filled
  | PendingCancel -> Some `order_status_pending_cancel
  | PendingReplace -> Some `order_status_pending_cancel_replace
  | _ -> None

let write_empty_order_update ?request_id w =
  let u = DTC.default_order_update () in
  u.total_num_messages <- Some 1l ;
  u.message_number <- Some 1l ;
  u.request_id <- request_id ;
  u.no_orders <- Some true ;
  u.order_update_reason <- Some `open_orders_request_response ;
  write_message w `order_update DTC.gen_order_update u

let write_open_order_update ?request_id ~msg_number ~nb_msgs ~status ~conn ~w (o : Order.t) =
  Option.iter o.account ~f:begin fun account ->
    let username = Int.Table.find_exn conn.Connection.usernames account in
    let status_reason = status, `open_orders_request_response in
    write_order_update ?request_id ~nb_msgs ~msg_number
      ~userid:account ~username ~status_reason w o
  end

let reject_open_orders_request ?request_id w k =
  let rej = DTC.default_open_orders_reject () in
  rej.request_id <- request_id ;
  Printf.ksprintf begin fun reject_text ->
    rej.reject_text <- Some reject_text ;
    write_message w `open_orders_reject DTC.gen_open_orders_reject rej
  end k

exception No_such_order

let get_open_orders ?orderID { Connection.order } userID =
  let open Option in
  match orderID with
  | None ->
      Int.Table.find_or_add order userID ~default:Uuid.Table.create |>
      Uuid.Table.fold ~init:[] ~f:begin fun ~key:uid ~data:o a ->
        match order_status_if_open o with
        | Some status -> (status, o) :: a
        | None -> a
      end
  | Some order_id ->
      match begin
        Int.Table.find order userID >>= fun table ->
        Uuid.Table.find table order_id >>= fun o ->
        order_status_if_open o >>| fun status -> [status, o]
      end with
      | None -> raise No_such_order
      | Some v -> v

let open_orders_request addr w msg =
  let conn = Connection.find_exn addr in
  let req = DTC.parse_open_orders_request msg in
  let trade_account = Option.value ~default:"" req.trade_account in
  Log.debug log_dtc "<- [%s] Open Orders Request (%s)" addr trade_account ;
  let orderID = Option.bind req.server_order_id
      ~f:(function "" -> None | uuid -> Some (Uuid.of_string uuid)) in
  match cut_trade_account trade_account with
  | None ->
      reject_open_orders_request ?request_id:req.request_id w
        "Trade Account must be speficied" ;
      Log.error log_bitmex
        "[%s] -> Open Orders Reject : trade account unspecified" addr
  | Some (_username, userID) ->
      match Int.Table.find conn.feeds userID with
      | None ->
          reject_open_orders_request ?request_id:req.request_id w
            "Internal error: No subscription for user" ;
          Log.error log_bitmex
            "[%s] -> Open Orders Reject (%s): internal error no sub for user"
            addr trade_account
      | Some feed -> don't_wait_for begin
          Feed.order_ready feed >>| fun () ->
          match get_open_orders ?orderID conn userID with
          | exception No_such_order ->
              reject_open_orders_request ?request_id:req.request_id w "No such order" ;
              Log.debug log_bitmex
                "[%s] -> Open Orders Response (%s): No such order" addr trade_account
          | [] ->
              write_empty_order_update ?request_id:req.request_id w ;
              Log.debug log_bitmex
                "[%s] -> Open Orders Response (%s): No Open Orders" addr trade_account
          | oos ->
              let nb_msgs = List.length oos in
              List.iteri oos ~f:begin fun i (status, o) ->
                write_open_order_update ?request_id:req.request_id ~nb_msgs ~msg_number:(succ i)
                  ~status ~conn ~w o
              end ;
              Log.debug log_bitmex
                "[%s] -> Open Orders Response (%s): %d Open Order(s)" addr trade_account nb_msgs
        end

let reject_current_positions_request ?request_id w k =
  let rej = DTC.default_current_positions_reject () in
  rej.request_id <- request_id ;
  Printf.ksprintf begin fun reject_text ->
    rej.reject_text <- Some reject_text ;
    write_message w `current_positions_reject DTC.gen_current_positions_reject rej
  end k

let write_current_position_update ?request_id ~msg_number ~nb_msgs ~conn ~w (p : Position.t) =
  let username = Int.Table.find_exn conn.Connection.usernames p.account in
  write_position_update ?request_id ~nb_msgs ~msg_number ~userid:p.account ~username w p

let write_no_positions ?trade_account ?request_id w =
  let u = DTC.default_position_update () in
  u.total_number_messages <- Some 1l ;
  u.message_number <- Some 1l ;
  u.request_id <- request_id ;
  u.no_positions <- Some true ;
  u.unsolicited <- Some false ;
  u.trade_account <- trade_account ;
  write_message w `position_update DTC.gen_position_update u

let get_open_positions position =
  String.Table.fold position ~init:(0, [])
    ~f:begin fun ~key:symbol ~data ((nb_open_ps, open_ps) as acc) ->
      if Option.value ~default:false data.Position.isOpen then succ nb_open_ps, data :: open_ps
      else acc
    end

let current_positions_request addr w msg =
  let ({ Connection.addr ; position ; feeds } as conn) = Connection.find_exn addr in
  let req = DTC.parse_current_positions_request msg in
  let trade_account = Option.value ~default:"" req.trade_account in
  Log.debug log_dtc "<- [%s] Current Positions Request (%s)" addr trade_account ;
  match cut_trade_account trade_account with
  | None ->
      reject_current_positions_request ?request_id:req.request_id w
        "Trade Account must be speficied" ;
      Log.error log_bitmex
        "[%s] -> Current Positions Reject: trade account unspecified" addr
  | Some (_username, userid) ->
      match Int.Table.find feeds userid with
      | None ->
          reject_current_positions_request ?request_id:req.request_id w
            "Internal error: No subscription for user" ;
          Log.error log_bitmex
            "[%s] -> Current Positions Reject (%s): internal error no sub for user"
            addr trade_account
      | Some feed -> don't_wait_for begin
          Feed.position_ready feed >>| fun () ->
          let position = Int.Table.find_or_add position ~default:String.Table.create userid in
          let nb_msgs, open_positions = get_open_positions position in
          List.iteri open_positions ~f:begin fun i p ->
            write_current_position_update ?request_id:req.request_id
              ~nb_msgs ~msg_number:(succ i) ~conn ~w p
          end ;
          if nb_msgs = 0 then
            write_no_positions ?trade_account:req.trade_account ?request_id:req.request_id w ;
          Log.debug log_dtc
            "-> [%s] Current Positions Request (%s): %d positions" addr trade_account nb_msgs
        end

let send_historical_order_fill
    (resp : DTC.Historical_order_fill_response.t) w { TradeHistory.e ; trade_account } message_number =
  let orderQty =
    Option.map2 e.cumQty e.leavesQty ~f:(fun a b -> Int.to_float (a + b)) in
  resp.trade_account <- Some trade_account ;
  resp.message_number <- Some message_number ;
  resp.symbol <- e.symbol ;
  resp.exchange <- Some !my_exchange ;
  resp.server_order_id <- Option.map ~f:Uuid.to_string e.orderID ;
  resp.price <- e.avgPx ;
  resp.quantity <- orderQty ;
  resp.date_time <- Option.map e.transactTime ~f:seconds_int64_of_ts ;
  resp.buy_sell <- Option.map e.side ~f:Side.of_string ;
  resp.unique_execution_id <- Some (Uuid.to_string e.execID) ;
  write_message w `historical_order_fill_response
    DTC.gen_historical_order_fill_response resp ;
  Int32.succ message_number

let send_historical_order_fills_response req addr trade_account w trades =
  let resp = DTC.default_historical_order_fill_response () in
  let nb_msgs = Uuid.Map.length trades in
  resp.total_number_messages <- Some (Int32.of_int_exn nb_msgs) ;
  resp.request_id <- req.DTC.Historical_order_fills_request.request_id ;
  let _ = Uuid.Map.fold trades ~init:1l ~f:begin fun ~key:_ ~data:t i ->
      try
        send_historical_order_fill resp w t i
      with _ ->
        Log.error log_bitmex "%s"
          (Sexplib.Sexp.to_string_hum (Execution.sexp_of_t t.e));
        Int32.succ i
    end in
  Log.debug log_dtc
    "-> [%s] Historical Order Fills Response (%s) (%d fills)" addr trade_account nb_msgs

let reject_historical_order_fills_request ?request_id w k =
  let rej = DTC.default_historical_order_fills_reject () in
  rej.request_id <- request_id ;
  Printf.ksprintf begin fun reject_text ->
    rej.reject_text <- Some reject_text ;
    write_message w `historical_order_fills_reject
      DTC.gen_historical_order_fills_reject rej
  end k

let write_no_historical_order_fills req w =
  let u = DTC.default_historical_order_fill_response () in
  u.total_number_messages <- Some 1l ;
  u.message_number <- Some 1l ;
  u.request_id <- req.DTC.Historical_order_fills_request.request_id ;
  u.server_order_id <- req.server_order_id ;
  u.trade_account <- req.trade_account ;
  u.no_order_fills <- Some true ;
  write_message w `historical_order_fill_response
    DTC.gen_historical_order_fill_response u

let historical_order_fills_request addr w msg =
    let ({ Connection.apikeys ; usernames } as conn) = Connection.find_exn addr in
    let req = DTC.parse_historical_order_fills_request msg in
    let orderID = Option.value ~default:"" req.server_order_id in
    let trade_account = Option.value ~default:"" req.trade_account in
    let min_ts = Option.map req.number_of_days ~f:begin fun i ->
        Time_ns.(sub (now ()) (Span.of_day (Int32.to_float i)))
      end in
    Log.debug log_dtc "<- [%s] Historical Order Fills Request (%s)" addr trade_account ;
    match orderID, cut_trade_account trade_account with
    | _, None ->
      reject_historical_order_fills_request ?request_id:req.request_id w
        "Trade Account must be speficied" ;
      Log.error log_bitmex
        "[%s] -> Historical Order Fills Reject: trade account unspecified" addr
    | "", Some (_username, userid) -> begin
        match Int.Table.mem apikeys userid with
        | false ->
            reject_historical_order_fills_request ?request_id:req.request_id w
              "No such account %s" trade_account
        | true -> don't_wait_for begin
            TradeHistory.get ?min_ts conn ~userid >>| function
            | Error err ->
                Log.error log_bitmex "%s" @@ Error.to_string_hum err ;
                reject_historical_order_fills_request ?request_id:req.request_id w
                  "Error fetching historical order fills from BitMEX"
            | Ok trades when Uuid.Map.is_empty trades ->
                write_no_historical_order_fills req w
            | Ok trades ->
                send_historical_order_fills_response req addr trade_account w trades
          end
      end
    | orderID, _ ->
        let orderID = Uuid.of_string orderID in
        match TradeHistory.get_one orderID with
        | None -> write_no_historical_order_fills req w
        | Some t ->
            send_historical_order_fills_response req addr trade_account w
              (Uuid.Map.singleton orderID t)

let trade_accounts_request addr w msg =
  let req = DTC.parse_trade_accounts_request msg in
  let conn = Connection.find_exn addr in
  Log.debug log_dtc "<- [%s] Trade Accounts Request" addr ;
  write_trade_accounts ?request_id:req.request_id conn

let reject_account_balance_request ?request_id w k =
  let rej = DTC.default_account_balance_reject () in
  rej.request_id <- request_id ;
  Printf.ksprintf begin fun reject_text ->
    rej.reject_text <- Some reject_text ;
    write_message w `account_balance_reject  DTC.gen_account_balance_reject rej
  end k

let write_no_balances req addr w =
  let resp = DTC.default_account_balance_update () in
  resp.request_id <- req.DTC.Account_balance_request.request_id ;
  resp.trade_account <- req.trade_account ;
  resp.total_number_messages <- Some 1l ;
  resp.message_number <- Some 1l ;
  resp.no_account_balances <- Some true ;
  resp.unsolicited <- Some false ;
  write_message w `account_balance_update  DTC.gen_account_balance_update resp ;
  Log.debug log_dtc "-> [%s] Account Balance Update: no balances" addr

let account_balance_request addr w msg =
  let { Connection.addr ; margin ; usernames ; feeds } = Connection.find_exn addr in
  let req = DTC.parse_account_balance_request msg in
  let trade_account = Option.value ~default:"" req.trade_account in
  Log.debug log_dtc "<- [%s] Account Balance Request (%s)" addr trade_account ;
  match cut_trade_account trade_account with
  | None ->
      reject_account_balance_request ?request_id:req.request_id w
        "Trade Account must be speficied" ;
      Log.error log_bitmex
        "[%s] -> Account Balance Reject: trade account unspecified" addr
  | Some (username, userid) ->
      match Int.Table.find feeds userid with
      | None ->
          reject_account_balance_request ?request_id:req.request_id w
            "Internal error: No subscription for user" ;
          Log.error log_bitmex
            "[%s] -> Account Balance Reject (%s): internal error no sub for user"
            addr trade_account
      | Some feed -> don't_wait_for begin
          Feed.margin_ready feed >>| fun () ->
          let margin =
            Int.Table.find_or_add margin ~default:String.Table.create userid in
          match
            Option.map (String.Table.find margin "XBt") ~f:(fun obj -> username, userid, obj)
          with
          | Some (username, userid, balance) ->
              write_balance_update ?request_id:req.request_id ~msg_number:1 ~nb_msgs:1
                ~username ~userid w balance ;
              Log.debug log_dtc "-> [%s] Account Balance Response: %s:%d" addr username userid
          | None ->
              write_no_balances req addr w
        end

let reject_order (req : DTC.Submit_new_single_order.t) w k =
  let rej = DTC.default_order_update () in
  rej.total_num_messages <- Some 1l ;
  rej.message_number <- Some 1l ;
  rej.trade_account <- req.trade_account ;
  rej.symbol <- req.symbol ;
  rej.exchange <- req.exchange ;
  rej.order_status <- Some `order_status_rejected ;
  rej.order_update_reason <- Some `new_order_rejected ;
  rej.client_order_id <- req.client_order_id ;
  rej.order_type <- req.order_type ;
  rej.buy_sell <- req.buy_sell ;
  rej.price1 <- req.price1 ;
  rej.price2 <- req.price2 ;
  rej.order_quantity <- req.quantity ;
  rej.time_in_force <- req.time_in_force ;
  rej.good_till_date_time <- req.good_till_date_time ;
  rej.free_form_text <- req.free_form_text ;
  Printf.ksprintf begin fun info_text ->
    rej.info_text <- Some info_text ;
    write_message w `order_update DTC.gen_order_update rej ;
  end k

let submit_order w ~key ~secret (req : DTC.Submit_new_single_order.t) stop_exec_inst =
  let symbol = Option.value_exn ~message:"symbol is undefined" req.symbol in
  let orderQty = Option.value_exn ~message:"qty is undefined" req.quantity in
  let orderQty = Int.of_float @@ match req.buy_sell with
    | Some `sell -> Float.neg orderQty
    | _ -> orderQty in
  let ordType = Option.value ~default:`order_type_unset req.order_type in
  let timeInForce = Option.value ~default:`tif_unset req.time_in_force in
  let price, stopPx =
    OrderType.to_price_stopPx ?p1:req.price1 ?p2:req.price2 ordType in
  let execInst = match ordType with
    | `order_type_market
    | `order_type_limit -> []
    | #OrderType.t -> [stop_exec_inst] in
  let displayQty, execInst = match timeInForce with
    | `tif_all_or_none -> Some 0, ExecInst.AllOrNone :: execInst
    | #DTC.time_in_force_enum -> None, execInst in
  let order =
    REST.Order.create
      ?displayQty
      ~execInst
      ?price
      ?stopPx
      ?clOrdID:req.client_order_id
      ?text:req.free_form_text
      ~symbol ~orderQty ~ordType ~timeInForce ()
  in
  REST.Order.submit_bulk
    ~log:log_bitmex
    ~testnet:!use_testnet ~key ~secret [order] >>| function
  | Ok _body -> ()
  | Error err ->
    let err_str = Error.to_string_hum err in
    reject_order req w "%s" err_str ;
    Log.error log_bitmex "%s" err_str

let submit_new_single_order addr w msg =
  let { Connection.addr; apikeys } as conn = Connection.find_exn addr in
  Log.debug log_dtc "<- [%s] Submit New Single Order" addr ;
  let req = DTC.parse_submit_new_single_order msg in
  let trade_account = Option.value ~default:"" req.trade_account in
  match cut_trade_account trade_account with
  | None ->
      reject_order req w "No trade account specified"
  | Some (username, userid) ->
      Int.Table.find_and_call apikeys userid ~if_not_found:begin fun _ ->
        reject_order req w "No API key for %s:%d" username userid ;
        Log.error log_bitmex "No API key for %s:%d" username userid
      end ~if_found:begin fun { id = key ; secret } ->
        (* TODO: Enable selection of execInst *)
        don't_wait_for (submit_order w ~key ~secret req LastPrice)
      end

let reject_cancel_replace_order
    ?symbol
    ?client_order_id
    ?server_order_id
    ?(order_status=`order_status_unspecified)
    (req : DTC.Cancel_replace_order.t) addr w k =
  let rej = DTC.default_order_update () in
  rej.symbol <- symbol ;
  rej.exchange <- Some !my_exchange ;
  rej.total_num_messages <- Some 1l ;
  rej.message_number <- Some 1l ;
  rej.client_order_id <- client_order_id ;
  rej.server_order_id <- Option.map server_order_id ~f:Uuid.to_string ;
  rej.order_update_reason <- Some `order_cancel_replace_rejected ;
  rej.order_status <- Some order_status ;
  Printf.ksprintf begin fun info_text ->
    rej.info_text <- Some info_text ;
    write_message w `order_update DTC.gen_order_update rej ;
    Log.debug log_dtc "-> [%s] Cancel Replace Rejected: %s" addr info_text
  end k

let amend_order addr w req key secret (o : Order.t) =
  let price1 = if req.DTC.Cancel_replace_order.price1_is_set = Some true then req.price1 else None in
  let price2 = if req.price2_is_set = Some true then req.price2 else None in
  let price, stopPx = match Option.map ~f:OrderType.of_string o.ordType with
  | None -> None, None
  | Some ordType -> OrderType.to_price_stopPx ?p1:price1 ?p2:price2 ordType in
  let amend = REST.Order.create_amend
      ?orderQty:(Option.map req.quantity ~f:Float.to_int)
      ?price
      ?stopPx
      ~orderID:o.orderID () in
  REST.Order.amend_bulk ~log:log_bitmex ~testnet:!use_testnet ~key ~secret [amend] >>| function
  | Ok (_resp, _orders) -> ()
  | Error err ->
      let err_str =
        match Error.to_exn err with
        | Failure msg -> msg
        | _ -> Error.to_string_hum err in
      reject_cancel_replace_order
        ?symbol:o.symbol
        ~server_order_id:o.orderID
        ?order_status:(Option.map o.ordStatus ~f:OrdStatus.(Fn.compose to_dtc of_string))
        req addr w "%s" err_str;
      Log.error log_bitmex "%s" err_str

let cancel_replace_order addr w msg =
  let ({ Connection.addr ; order ; apikeys } as conn) = Connection.find_exn addr in
  Log.debug log_dtc "<- [%s] Cancel Replace Order" addr ;
  let req = DTC.parse_cancel_replace_order msg in
  match Option.(req.server_order_id >>| Uuid.of_string >>= Connection.find_order order) with
  | None ->
      let client_order_id = Option.value ~default:"" req.client_order_id in
      let server_order_id = Option.value ~default:"" req.server_order_id in
      reject_cancel_replace_order
        ?client_order_id:req.client_order_id
        ~order_status:`order_status_rejected
        req addr w "order %s (%s) not found" server_order_id client_order_id
  | Some o ->
      let order_type = Option.value ~default:`order_type_unset req.order_type in
      let time_in_force = Option.value ~default:`tif_unset req.time_in_force in
      if order_type <> `order_type_unset then
        reject_cancel_replace_order
          ?symbol:o.symbol ~server_order_id:o.orderID req addr w
          "Modification of ordType is not supported by BitMEX"
      else if time_in_force <> `tif_unset then
        reject_cancel_replace_order
          ?symbol:o.symbol ~server_order_id:o.orderID req addr w
          "Modification of timeInForce is not supported by BitMEX"
      else
      let open Option in
      iter (o.account >>= Int.Table.find apikeys) ~f:begin fun { ApiKey.id = key ; secret } ->
        don't_wait_for (amend_order addr w req key secret o)
      end

let reject_cancel_order
    ?symbol
    ?client_order_id
    ?server_order_id
    ?(order_status=`order_status_unspecified)
    (req : DTC.Cancel_order.t) addr w k =
  let rej = DTC.default_order_update () in
  rej.symbol <- symbol ;
  rej.exchange <- Some !my_exchange ;
  rej.total_num_messages <- Some 1l ;
  rej.message_number <- Some 1l ;
  rej.client_order_id <- client_order_id ;
  rej.server_order_id <- Option.map server_order_id ~f:Uuid.to_string ;
  rej.order_status <- Some order_status ;
  rej.order_update_reason <- Some `order_cancel_rejected ;
  Printf.ksprintf begin fun info_text ->
    rej.info_text <- Some info_text ;
    write_message w `order_update DTC.gen_order_update rej ;
    Log.debug log_dtc "-> [%s] Cancel Rejected: %s" addr info_text
  end k

let cancel_order req addr w key secret (o : Order.t) =
  REST.Order.cancel
    ~extract_exn:true ~log:log_bitmex ~testnet:!use_testnet
    ~key ~secret ~orderIDs:[o.orderID] () >>| function
  | Ok (_resp, _orders) -> ()
  | Error err ->
      let err_str =
        match Error.to_exn err with
        | Failure msg -> msg
        | _ -> Error.to_string_hum err in
      reject_cancel_order
        ?symbol:o.symbol
        ~server_order_id:o.orderID
        ?order_status:(Option.map o.ordStatus ~f:OrdStatus.(Fn.compose to_dtc of_string))
        req addr w "%s" err_str;
      Log.error log_bitmex "%s" err_str

let cancel_order addr w msg =
    let ({ Connection.addr ; order ; apikeys } as conn) = Connection.find_exn addr in
    Log.debug log_dtc "<- [%s] Cancel Order" addr ;
    let req = DTC.parse_cancel_order msg in
    match Option.(req.server_order_id >>| Uuid.of_string >>= Connection.find_order order) with
    | None ->
        let client_order_id = Option.value ~default:"" req.client_order_id in
        let server_order_id = Option.value ~default:"" req.server_order_id in
        reject_cancel_order
          ?client_order_id:req.client_order_id
          ~order_status:`order_status_rejected
          req addr w "order %s (%s) not found" server_order_id client_order_id
    | Some o ->
        match
          Option.value_map o.ordStatus ~default:(OrdStatus.Unknown "") ~f:OrdStatus.of_string
        with
        | Canceled ->
            reject_cancel_order
              ?symbol:o.symbol
              ?client_order_id:req.client_order_id
              ~order_status:`order_status_canceled
              req addr w "Order has already been canceled"
        | PendingCancel ->
            reject_cancel_order
              ?symbol:o.symbol
              ?client_order_id:req.client_order_id
              ~order_status:`order_status_pending_cancel
              req addr w "Order is pending cancel"
        | _ ->
            let open Option in
            iter (o.account >>= Int.Table.find apikeys) ~f:begin fun { ApiKey.id = key ; secret } ->
              don't_wait_for @@ cancel_order req addr w key secret o
            end

let rec handle_chunk addr w consumed buf ~pos ~len =
  if len < 2 then return @@ `Consumed (consumed, `Need_unknown)
  else
  let msglen = Bigstring.unsafe_get_int16_le buf ~pos in
  (* Log.debug log_dtc "handle_chunk: pos=%d len=%d, msglen=%d" pos len msglen; *)
  if len < msglen then return @@ `Consumed (consumed, `Need msglen)
  else begin
    let msgtype_int = Bigstring.unsafe_get_int16_le buf ~pos:(pos+2) in
    let msgtype : DTC.dtcmessage_type =
      DTC.parse_dtcmessage_type (Piqirun.Varint msgtype_int) in
    let msg_str = Bigstring.To_string.subo buf ~pos:(pos+4) ~len:(msglen-4) in
    let msg = Piqirun.init_from_string msg_str in
    begin match msgtype with
    | `encoding_request ->
        begin match (Dtc_pb.Encoding.read (Bigstring.To_string.subo buf ~pos ~len:16)) with
        | None -> Log.error log_dtc "Invalid encoding request received"
        | Some msg -> encoding_request addr w msg
        end
    | `logon_request -> logon_request addr w msg
    | `heartbeat -> heartbeat addr w msg
    | `security_definition_for_symbol_request -> security_definition_request addr w msg
    | `market_data_request -> market_data_request addr w msg
    | `market_depth_request -> market_depth_request addr w msg
    | `open_orders_request -> open_orders_request addr w msg
    | `current_positions_request -> current_positions_request addr w msg
    | `historical_order_fills_request -> historical_order_fills_request addr w msg
    | `trade_accounts_request -> trade_accounts_request addr w msg
    | `account_balance_request -> account_balance_request addr w msg
    | `submit_new_single_order -> submit_new_single_order addr w msg
    | `cancel_order -> cancel_order addr w msg
    | `cancel_replace_order -> cancel_replace_order addr w msg
    | #DTC.dtcmessage_type ->
        Log.error log_dtc "Unknown msg type %d" msgtype_int
    end ;
    handle_chunk addr w (consumed + msglen) buf (pos + msglen) (len - msglen)
  end

let dtcserver ~server ~port =
  let server_fun addr r w =
    don't_wait_for begin
      Condition.wait ws_feed_connected >>= fun () ->
      Deferred.all_unit [Writer.close w ; Reader.close r]
    end ;
    let addr = Socket.Address.Inet.to_string addr in
    let on_connection_io_error exn =
      Connection.remove addr ;
      Log.error log_dtc "on_connection_io_error (%s): %s" addr Exn.(to_string exn)
    in
    let cleanup () =
      Log.info log_dtc "client %s disconnected" addr ;
      begin match Connection.find addr with
      | None -> ()
      | Some conn ->
          Connection.purge conn ;
          Pipe.write_without_pushback client_deleted_w conn
      end ;
      Connection.remove addr ;
      Deferred.all_unit [Writer.close w; Reader.close r]
    in
    Deferred.ignore @@ Monitor.protect ~finally:cleanup begin fun () ->
      Monitor.detach_and_iter_errors Writer.(monitor w) ~f:on_connection_io_error;
      Reader.(read_one_chunk_at_a_time r ~handle_chunk:(handle_chunk addr w 0 ))
    end in
  let on_handler_error_f addr exn =
    Log.error log_dtc "on_handler_error (%s): %s"
      Socket.Address.(to_string addr) Exn.(to_string exn)
  in
  Conduit_async.serve
    ~on_handler_error:(`Call on_handler_error_f)
    server (Tcp.Where_to_listen.of_port port) server_fun

let update_trade { Trade.symbol; timestamp; price; size; side } =
  let side = Option.value_map ~default:`buy_sell_unset ~f:Side.of_string side in
  let price = Option.value ~default:0. price in
  let size = Option.value ~default:0 size in
  Log.debug log_bitmex "trade %s %s %f %d" symbol (Side.show side) price size;
  match side, Instr.find symbol with
  | `buy_sell_unset, _ -> ()
  | _, None ->
    Log.error log_bitmex "update_trade: found no instrument for %s" symbol
  | _, Some instr ->
    instr.last_trade_price <- price;
    instr.last_trade_size <- size;
    instr.last_trade_ts <- timestamp;
    (* Send trade updates to subscribers. *)
    let at_bid_or_ask =
      match side with
      | `buy -> `at_ask
      | `sell -> `at_bid
      | `buy_sell_unset -> `bid_ask_unset in
    let u = DTC.default_market_data_update_trade () in
    u.at_bid_or_ask <- Some at_bid_or_ask ;
    u.price <- Some price ;
    u.volume <- Some (Int.to_float size) ;
    u.date_time <- Some (seconds_float_of_ts timestamp) ;
    let on_connection { Connection.addr; w; subs } =
      let on_symbol_id symbol_id =
        u.symbol_id <- Some symbol_id ;
        write_message w `market_data_update_trade
          DTC.gen_market_data_update_trade u ;
        Log.debug log_dtc "-> [%s] trade %s %s %f %d"
          addr symbol (Side.show side) price size
      in
      Option.iter String.Table.(find subs symbol) ~f:on_symbol_id
    in
    Connection.iter ~f:on_connection

type bitmex_th = {
  th: unit Deferred.t ;
  ws: Yojson.Safe.json Pipe.Reader.t ;
}

let close_bitmex_ws { ws } = Pipe.close_read ws

let on_update { Bmex_ws.Response.Update.table ; action ; data } =
  match action, table, data with
  | Update, Instrument, instrs ->
    if Ivar.is_full Instr.initialized then
      List.iter instrs ~f:(Fn.compose Instr.update Instrument.of_yojson)
  | Delete, Instrument, instrs ->
    if Ivar.is_full Instr.initialized then
      List.iter instrs ~f:(Fn.compose Instr.delete Instrument.of_yojson)
  | _, Instrument, instrs ->
    List.iter instrs ~f:(Fn.compose Instr.insert Instrument.of_yojson);
    Ivar.fill_if_empty Instr.initialized ()
  | _, OrderBookL2, depths ->
    let depths = List.map depths ~f:OrderBookL2.of_yojson in
    let depths = List.group depths
        ~break:(fun { symbol } { symbol=symbol' } -> symbol <> symbol')
    in
    don't_wait_for begin
      Ivar.read Instr.initialized >>| fun () ->
      List.iter depths ~f:begin function
        | [] -> ()
        | h::t as ds ->
          Log.debug log_bitmex "depth update %s" h.symbol;
          List.iter ds ~f:(Books.update action)
      end;
      Ivar.fill_if_empty Books.initialized ()
    end
  | _, Trade, trades ->
    let open Trade in
    don't_wait_for begin
      Ivar.read Instr.initialized >>| fun () ->
      List.iter trades ~f:(Fn.compose update_trade Trade.of_yojson)
    end
  | _, Quote, quotes ->
    List.iter quotes ~f:(Fn.compose Quotes.update Quote.of_yojson) ;
    Ivar.fill_if_empty Quotes.initialized ()
  | _, table, json ->
    Log.error log_bitmex "Unknown/ignored BitMEX DB table %s or wrong json %s"
      (Bmex_ws.Topic.to_string table)
      Yojson.Safe.(to_string @@ `List json)

let subscribe_topics ?(topic="") ~id ~topics =
  let open Bmex_ws in
  let payload =
    Request.(subscribe (List.map topics ~f:Sub.create) |> to_yojson) in
  Bmex_ws.MD.message id topic payload

let stream_id ~uuid ~addr ~id =
  uuid ^ "|" ^ addr ^ "|" ^ Int.to_string id

let subscribe_client ?(topic="") ~uuid ~addr ~id () =
  Log.debug log_bitmex "Subscribe Client %s %s %d" uuid addr id ;
  let id = stream_id ~uuid ~addr ~id in
  Bmex_ws.MD.(subscribe ~id ~topic |> to_yojson)

let unsubscribe_client ?(topic="") ~uuid ~addr ~id () =
  Log.debug log_bitmex "Unsubscribe Client %s %s %d" uuid addr id ;
  let id = stream_id ~uuid ~addr ~id in
  Bmex_ws.MD.(unsubscribe ~id ~topic |> to_yojson)

type stream =
  | Server
  | Zombie of string * int
  | Client of (Connection.t * int)

let conn_userid_of_stream_id stream_id =
  match String.split ~on:'|' stream_id with
  | [_; addr; userId] ->
      let userId = Int.of_string userId in
      Option.value_map (Connection.find addr)
        ~default:(Zombie (addr, userId))
        ~f:(fun conn -> Client (conn, userId))
  | _ -> Server

let bitmex_topics = Bmex_ws.Topic.[Instrument; Quote; OrderBookL2; Trade]
let client_topics = Bmex_ws.Topic.[Order; Execution; Position; Margin]

let on_ws_msg to_ws_w my_uuid msg =
  let open Bmex_ws in
  match MD.of_yojson ~log:log_bitmex msg with
  | Subscribe _ -> ()
  | Unsubscribe { id ; topic } -> begin
      match conn_userid_of_stream_id id with
      | Server ->
          Log.error log_bitmex "Got Unsubscribe message for server"
      | Zombie (addr, userId) ->
          Log.debug log_bitmex "Got Unsubscribe message for zombie %s %d" addr userId
      | Client (conn, userid) ->
          Int.Table.remove conn.subscriptions userid ;
          Log.debug log_bitmex "Got Unsubscribe message for %s" id
    end
  | Message { stream = { id ; topic } ; payload } -> begin
      match Response.of_yojson ~log:log_bitmex payload,
            conn_userid_of_stream_id id with
      (* Server *)
      | Response.Welcome _, Server ->
          Pipe.write_without_pushback to_ws_w @@
          MD.to_yojson @@ subscribe_topics my_uuid bitmex_topics
      | Error err, Server ->
          Log.error log_bitmex "BitMEX: error %s" err
      | Response { subscribe = Some { topic ; symbol = Some sym } }, Server ->
          Log.info log_bitmex
            "BitMEX: subscribed to %s:%s" (Topic.show topic) sym
      | Response { subscribe = Some { topic; symbol = None }}, Server ->
          Log.info log_bitmex
            "BitMEX: subscribed to %s" (Topic.show topic)
      | Update update, Server -> on_update update

      (* Clients *)
      | Welcome _, Client (conn, userid) ->
          let { ApiKey.id = key ; secret } =
            Int.Table.find_exn conn.Connection.apikeys userid in
          Pipe.write_without_pushback to_ws_w
            MD.(auth ~id ~topic:"" ~key ~secret |> to_yojson)
      | Error err, Client (conn, userid) ->
          Log.error log_bitmex "[%s] %d: error %s" conn.addr userid err ;
      | Response { request = AuthKey _ ; success}, Client (conn, userid) ->
          (* Subscription of of userid for conn has succeeded. *)
          Int.Table.set conn.subscriptions userid () ;
          Log.debug log_bitmex "[%s] %d: subscribe to topics" conn.addr userid;
          Pipe.write_without_pushback to_ws_w @@
          MD.to_yojson @@ subscribe_topics id client_topics
      | Response { subscribe = Some { topic = subscription } }, Client (conn, userid) ->
          Log.info log_bitmex "[%s] %d: subscribed to %s"
            conn.addr userid (Topic.show subscription)
      | Response _, Client (conn, userid) ->
          Log.error log_bitmex "[%s] %d: unexpected response %s"
            conn.addr userid (Yojson.Safe.to_string payload)
      | Update update, Client (conn, userid) ->
          let client_update = { Connection.userid ; update } in
          Pipe.write_without_pushback conn.ws_w client_update

      | _, Zombie (addr, userId) ->
          Log.error log_bitmex
            "Got a message on zombie subscription %s %d, unsubscribing" addr userId ;
          Pipe.write_without_pushback to_ws_w @@
          Bmex_ws.MD.(unsubscribe ~id ~topic |> to_yojson)
      | _ -> ()
    end

let bitmex_ws () =
  let open Bmex_ws in
  let to_ws, to_ws_w = Pipe.create () in
  let my_uuid = Uuid.(create () |> to_string) in
  let connected = Condition.create () in
  let rec resubscribe () =
    Condition.wait connected >>= fun () ->
    Condition.broadcast ws_feed_connected () ;
    Pipe.write to_ws_w MD.(subscribe ~id:my_uuid ~topic:"" |> to_yojson) >>= fun () ->
    Connection.iter ~f:begin fun { subscriptions ; feeds } ->
      Int.Table.clear subscriptions ;
      Int.Table.clear feeds
    end ;
    resubscribe ()
  in
  don't_wait_for begin
    Pipe.iter_without_pushback
      ~continue_on_error:true new_client_accepted ~f:begin fun { to_bitmex_r } ->
      don't_wait_for begin
        Pipe.iter ~continue_on_error:true to_bitmex_r ~f:begin function
        | Subscribe { id; addr } ->
            Pipe.write to_ws_w (subscribe_client ~uuid:my_uuid ~addr ~id ())
        | Unsubscribe { id; addr } ->
            Pipe.write to_ws_w  (unsubscribe_client ~uuid:my_uuid ~addr ~id ())
        end
      end
    end
  end ;
  don't_wait_for begin
    Pipe.iter client_deleted ~f:begin fun { Connection.addr ; usernames } ->
      let usernames = Int.Table.to_alist usernames in
      Deferred.List.iter usernames ~how:`Sequential ~f:begin fun (id, _) ->
        Pipe.write to_ws_w (unsubscribe_client ~uuid:my_uuid ~addr ~id ())
      end
    end
  end ;
  don't_wait_for @@ resubscribe ();
  let ws = open_connection ~connected ~to_ws ~log:log_ws
      ~testnet:!use_testnet ~md:true ~topics:[] () in
  let th =
    Monitor.handle_errors
      (fun () -> Pipe.iter_without_pushback
          ~continue_on_error:true ws ~f:(on_ws_msg to_ws_w my_uuid))
      (fun exn -> Log.error log_bitmex "%s" @@ Exn.to_string exn) in
  { th ; ws }

let main
    tls testnet port daemon
    pidfile logfile loglevel
    ll_ws ll_dtc ll_bitmex crt_path key_path () =
  let pidfile = if testnet then add_suffix pidfile "_testnet" else pidfile in
  let logfile = if testnet then add_suffix logfile "_testnet" else logfile in
  let run ~server ~port =
    Log.info log_bitmex "WS feed starting";
    let bitmex_th = bitmex_ws () in
    Deferred.List.iter ~how:`Parallel ~f:Ivar.read
      [Instr.initialized; Books.initialized; Quotes.initialized] >>= fun () ->
    dtcserver ~server ~port >>= fun dtc_server ->
    Log.info log_dtc "DTC server started";
    Deferred.all_unit [Tcp.Server.close_finished dtc_server; bitmex_th.th]
  in

  (* start initilization code *)
  if testnet then begin
    use_testnet := testnet;
    base_uri := Uri.of_string "https://testnet.bitmex.com";
    my_exchange := "BMEXT"
  end;

  Log.set_level log_dtc @@ loglevel_of_int @@ max loglevel ll_dtc;
  Log.set_level log_bitmex @@ loglevel_of_int @@ max loglevel ll_bitmex;
  Log.set_level log_ws @@ loglevel_of_int ll_ws;

  if daemon then Daemon.daemonize ~cd:"." ();
  stage begin fun `Scheduler_started ->
    Unix.mkdir ~p:() (Filename.dirname pidfile) >>= fun () ->
    Unix.mkdir ~p:() (Filename.dirname logfile) >>= fun () ->
    Lock_file.create_exn ~unlink_on_exit:true pidfile >>= fun () ->
    Writer.open_file ~append:true logfile >>= fun log_writer ->
    Log.(set_output log_dtc Output.[stderr (); writer `Text log_writer]);
    Log.(set_output log_bitmex Output.[stderr (); writer `Text log_writer]);
    Log.(set_output log_ws Output.[stderr (); writer `Text log_writer]);
    conduit_server ~tls ~crt_path ~key_path >>= fun server ->
    loop_log_errors ~log:log_dtc (fun () -> run ~server ~port) >>= fun () ->
    Shutdown.exit 0
  end

let command =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-tls" no_arg ~doc:" Use TLS (see also -crt-file, -key-file)"
    +> flag "-testnet" no_arg ~doc:" Use testnet"
    +> flag "-port" (optional_with_default 5567 int) ~doc:"int TCP port to use (5567)"
    +> flag "-daemon" no_arg ~doc:" Run as a daemon"
    +> flag "-pidfile" (optional_with_default "run/bitmex.pid" string) ~doc:"filename Path of the pid file (run/bitmex.pid)"
    +> flag "-logfile" (optional_with_default "log/bitmex.log" string) ~doc:"filename Path of the log file (log/bitmex.log)"
    +> flag "-loglevel" (optional_with_default 1 int) ~doc:"1-3 global loglevel"
    +> flag "-loglevel-ws" (optional_with_default 1 int) ~doc:"1-3 loglevel for the websocket library"
    +> flag "-loglevel-dtc" (optional_with_default 1 int) ~doc:"1-3 loglevel for DTC"
    +> flag "-loglevel-bitmex" (optional_with_default 1 int) ~doc:"1-3 loglevel for BitMEX"
    +> flag "-crt-file" (optional_with_default "ssl/bitmex.crt" string) ~doc:"filename TLS certificate file to use (ssl/bitmex.crt)"
    +> flag "-key-file" (optional_with_default "ssl/bitmex.key" string) ~doc:"filename TLS key file to use (ssl/bitmex.key)"
  in
  Command.Staged.async_spec ~summary:"BitMEX bridge" spec main

let () = Command.run command

(*---------------------------------------------------------------------------
   Copyright (c) 2016 Vincent Bernardoff

   Permission to use, copy, modify, and/or distribute this software for any
   purpose with or without fee is hereby granted, provided that the above
   copyright notice and this permission notice appear in all copies.

   THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
   WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
   MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
   ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
   WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
   ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
   OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
  ---------------------------------------------------------------------------*)
