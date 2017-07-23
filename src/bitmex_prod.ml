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
open Bmex

module DTC = Dtc_pb.Dtcprotocol_piqi
module WS = Bmex_ws
module REST = Bmex_rest

let write_message w (typ : DTC.dtcmessage_type) gen msg =
  let typ =
    Piqirun.(DTC.gen_dtcmessage_type typ |> to_string |> init_from_string |> int_of_varint) in
  let msg = (gen msg |> Piqirun.to_string) in
  let header = Bytes.create 4 in
  Binary_packing.pack_unsigned_16_little_endian ~buf:header ~pos:0 (4 + String.length msg) ;
  Binary_packing.pack_unsigned_16_little_endian ~buf:header ~pos:2 typ ;
  Writer.write w header ;
  Writer.write w msg

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

module Connection = struct
  type subscribe_msg =
    | Subscribe of { addr: string; id: int }
    | Unsubscribe of { addr: string; id: int }

  type apikey = {
    key: string;
    secret: string;
  }

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
    to_client_r: subscribe_msg Pipe.Reader.t;
    to_client_w: subscribe_msg Pipe.Writer.t;
    key: string;
    secret: string;
    position: RespObj.t IS.Table.t; (* indexed by account, symbol *)
    margin: RespObj.t IS.Table.t; (* indexed by account, currency *)
    order: RespObj.t Uuid.Table.t Int.Table.t; (* indexed by account, then orderID *)
    mutable dropped: int;
    subs: int32 String.Table.t;
    rev_subs: string Int32.Table.t ;
    subs_depth: int32 String.Table.t;
    rev_subs_depth: string Int32.Table.t ;
    send_secdefs: bool;

    apikeys : apikey Int.Table.t; (* indexed by account *)
    usernames : string Int.Table.t; (* indexed by account *)
    accounts : int String.Table.t; (* indexed by SC username *)
    mutable need_resubscribe: bool;
  }

  let create ~addr ~w ~key ~secret ~send_secdefs =
    let ws_r, ws_w = Pipe.create () in
    let to_bitmex_r, to_bitmex_w = Pipe.create () in
    let to_client_r, to_client_w = Pipe.create () in
    {
      addr ;
      w ;
      ws_r ;
      ws_w ;
      to_bitmex_r ;
      to_bitmex_w ;
      to_client_r ;
      to_client_w ;
      key ;
      secret ;
      position = IS.Table.create () ;
      margin = IS.Table.create () ;
      order = Int.Table.create () ;
      subs = String.Table.create () ;
      rev_subs = Int32.Table.create () ;
      subs_depth = String.Table.create () ;
      rev_subs_depth = Int32.Table.create () ;
      send_secdefs ;
      apikeys = Int.Table.create () ;
      usernames = Int.Table.create () ;
      accounts = String.Table.create () ;

      dropped = 0 ;
      need_resubscribe = false ;
    }

  let purge { ws_r; to_bitmex_r; to_client_r } =
    Pipe.close_read ws_r;
    Pipe.close_read to_bitmex_r;
    Pipe.close_read to_client_r

  let active : t String.Table.t = String.Table.create ()
  let to_alist () = String.Table.to_alist active

  let find = String.Table.find active
  let find_exn = String.Table.find_exn active
  let set = String.Table.set active
  let remove = String.Table.remove active

  let iter = String.Table.iter active
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

  let update action { OrderBook.L2.symbol; id; side; size; price } =
    (* find_exn cannot raise here *)
    let bids = String.Table.find_or_add bids symbol ~default:Int.Table.create in
    let asks = String.Table.find_or_add asks symbol ~default:Int.Table.create in
    let table =
      match side with
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
        match side with
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

module Instrument = struct
  open RespObj
  let is_index symbol = symbol.[0] = '.'
  let to_secdef ~testnet t =
    let symbol = string_exn t "symbol" in
    let index = is_index symbol in
    let exchange =
      string_exn t "reference"
      ^ (if testnet && not index then "T" else "")
    in
    let tickSize = float_exn t "tickSize" in
    let expiration_date = Option.map (string t "expiry") ~f:(fun time ->
        Time_ns.(of_string time |>
                 to_int_ns_since_epoch |>
                 (fun t -> t / 1_000_000_000) |>
                 Int32.of_int_exn)) in
    let secdef = DTC.default_security_definition_response () in
    secdef.symbol <- Some symbol ;
    secdef.exchange <- Some exchange ;
    secdef.security_type <-
      Some (if index then `security_type_index else `security_type_future) ;
    secdef.min_price_increment <- Some tickSize ;
    secdef.currency_value_per_increment <- Some tickSize ;
    secdef.price_display_format <- Some (price_display_format_of_ticksize tickSize) ;
    secdef.has_market_depth_data <- Some (not index) ;
    secdef.underlying_symbol <- Some (string_exn t "underlyingSymbol") ;
    secdef.updates_bid_ask_only <- Some false ;
    secdef.security_expiration_date <- expiration_date ;
    secdef

  type t = {
    mutable instrObj: RespObj.t;
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
      ~instrObj ~secdef () = {
    instrObj ; secdef ;
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

  let delete instrObj =
    let instrObj = RespObj.of_json instrObj in
    let symbol = RespObj.(string_exn instrObj "symbol") in
    remove symbol ;
    Log.info log_bitmex "deleted instrument %s" symbol

  let insert instrObj =
    let instrObj = RespObj.of_json instrObj in
    let symbol = RespObj.string_exn instrObj "symbol" in
    let secdef = to_secdef ~testnet:!use_testnet instrObj in
    let instr = create ~instrObj ~secdef () in
    set symbol instr;
    Log.info log_bitmex "inserted instrument %s" symbol;
    (* Send secdef response to connections. *)
    let on_connection { Connection.addr; w } =
      secdef.is_final_message <- Some true ;
      write_message w `security_definition_response
        DTC.gen_security_definition_response secdef
    in
    Connection.iter ~f:on_connection

  let send_instr_update_msgs w instr symbol_id =
    let open RespObj in
    Option.iter (int64 instr "volume") ~f:begin fun volume ->
      let msg = DTC.default_market_data_update_session_volume () in
      msg.symbol_id <- Some symbol_id ;
      msg.volume <- Some Int64.(to_float volume) ;
      write_message w `market_data_update_session_volume
        DTC.gen_market_data_update_session_volume msg
    end ;
    Option.iter (float instr "lowPrice") ~f:begin fun low ->
      let msg = DTC.default_market_data_update_session_low () in
      msg.symbol_id <- Some symbol_id ;
      msg.price <- Some low ;
      write_message w `market_data_update_session_low
        DTC.gen_market_data_update_session_low msg
    end ;
    Option.iter (float instr "highPrice") ~f:begin fun high ->
      let msg = DTC.default_market_data_update_session_high () in
      msg.symbol_id <- Some symbol_id ;
      msg.price <- Some high ;
      write_message w `market_data_update_session_high
        DTC.gen_market_data_update_session_high msg
    end ;
    Option.iter (int64 instr "openInterest") ~f:begin fun open_interest ->
      let msg = DTC.default_market_data_update_open_interest () in
      msg.symbol_id <- Some symbol_id ;
      msg.open_interest <- Some (Int64.to_int32_exn open_interest) ;
      write_message w `market_data_update_open_interest
        DTC.gen_market_data_update_open_interest msg
    end ;
    Option.iter (float instr "prevClosePrice")~f:begin fun prev_close ->
      let msg = DTC.default_market_data_update_session_open () in
      msg.symbol_id <- Some symbol_id ;
      msg.price <- Some prev_close ;
      write_message w `market_data_update_session_open
        DTC.gen_market_data_update_session_open msg
    end

  let update instrObj =
    let instrObj = RespObj.of_json instrObj in
    let symbol = RespObj.string_exn instrObj "symbol" in
    match find symbol with
    | None ->
      Log.error log_bitmex "update_instr: unable to find %s" symbol;
    | Some instr ->
      instr.instrObj <- RespObj.merge instr.instrObj instrObj;
      Log.debug log_bitmex "updated instrument %s" symbol;
      (* Send messages to subscribed clients according to the type of update. *)
      let on_connection { Connection.addr; w; subs } =
        let on_symbol_id symbol_id =
          send_instr_update_msgs w instrObj symbol_id;
          Log.debug log_dtc "-> [%s] instrument %s" addr symbol
        in
        Option.iter String.Table.(find subs symbol) ~f:on_symbol_id
      in
      Connection.iter ~f:on_connection
end

module Order = struct
  exception Found of Uuid.t * int * RespObj.t
  let find order uuid = try
    Int.Table.iteri order ~f:begin fun ~key ~data ->
      match Uuid.Table.find data uuid with
      | Some o -> raise (Found (uuid, key, o))
      | None -> ()
    end;
    None
  with Found (uuid, account, o) -> Some (uuid, account, o)
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
    u.date_time <- seconds_int32_of_ts merged_q.timestamp ;
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

let send_heartbeat { Connection.addr ; w } span =
  let msg = DTC.default_heartbeat () in
  Clock_ns.every
    ~stop:(Writer.close_started w)
    ~continue_on_error:false span
    begin fun () ->
      (* Log.debug log_dtc "-> [%s] HB" addr ; *)
      write_message w `heartbeat DTC.gen_heartbeat msg
    end

let fail_ordStatus_execType ~ordStatus ~execType =
  invalid_argf
    "Wrong ordStatus/execType pair: %s, %s"
    (OrdStatus.show ordStatus)
    (ExecType.show execType)
    ()

let status_reason_of_execType_ordStatus e =
  let ordStatus = RespObj.(string_exn e "ordStatus") |> OrdStatus.of_string in
  let execType = RespObj.(string_exn e "execType") |> ExecType.of_string in
  match ordStatus, execType with

  | New, New
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

let write_order_update ?request_id ?(nb_msgs=1) ?(msg_number=1) ~userid ~username ~status ~reason w o =
  let open RespObj in
  let u = DTC.default_order_update () in
  let price = float_or_null_exn ~default:Float.max_finite_value o "price" in
  let stopPx = float_or_null_exn ~default:Float.max_finite_value o "stopPx" in
  let side = Option.map (string o "side") ~f:Side.of_string in
  let ordType = Option.map (string o "ordType") ~f:OrderType.of_string in
  let timeInForce = Option.map (string o "timeInForce")~f:TimeInForce.of_string in
  let ts = Option.map (string o "transactTime")
      ~f:(Fn.compose seconds_int64_of_ts Time_ns.of_string) in
  let p1, p2 = OrderType.to_p1_p2 ~stopPx ~price
      (Option.value ~default:`order_type_unset ordType) in
  u.request_id <- request_id ;
  u.total_num_messages <- Some (Int32.of_int_exn nb_msgs) ;
  u.message_number <- Some (Int32.of_int_exn msg_number) ;
  u.symbol <- (string o "symbol") ;
  u.exchange <- Some !my_exchange ;
  u.client_order_id <- string o "clOrdID" ;
  u.server_order_id <- string o "orderID" ;
  u.exchange_order_id <- string o "orderID" ;
  u.order_type <- ordType ;
  u.order_status <- Some status ;
  u.order_update_reason <- Some reason ;
  u.buy_sell <- side ;
  u.price1 <- p1 ;
  u.price2 <- p2 ;
  u.time_in_force <- timeInForce ;
  u.order_quantity <- Option.map (int64 o "orderQty") ~f:Int64.to_float ;
  u.filled_quantity <- Option.map (int64 o "cumQty") ~f:Int64.to_float ;
  u.remaining_quantity <- Option.map (int64 o "leavesQty") ~f:Int64.to_float ;
  u.average_fill_price <- (float o "avgPx") ;
  u.last_fill_price <- (float o "lastPx") ;
  u.last_fill_date_time <- ts ;
  u.last_fill_quantity <- Option.map ~f:Int64.to_float (int64 o "lastQty") ;
  u.last_fill_execution_id <- string o "execID" ;
  u.trade_account <- Some (trade_accountf ~userid ~username) ;
  u.free_form_text <- string o "text" ;
  write_message w `order_update DTC.gen_order_update u

let write_order_update ?request_id ?(nb_msgs=1) ?(msg_number=1) ?status_reason ~userid ~username w o =
  match status_reason with
  | Some (status, reason) ->
      write_order_update ?request_id ~status ~reason ~userid ~username w o
  | None ->
      match status_reason_of_execType_ordStatus o with
      | exception Exit -> ()
      | exception Invalid_argument msg ->
          Log.error log_bitmex "Not sending order update for %s" msg ;
          ()
      | status, reason ->
          write_order_update ?request_id ~status ~reason ~userid ~username w o

let write_position_update ?request_id ?(nb_msgs=1) ?(msg_number=1) ~userid ~username w p =
  let symbol = RespObj.string p "symbol" in
  let avgEntryPrice = RespObj.float p "avgEntryPrice" in
  let currentQty = RespObj.int p "currentQty" in
  let u = DTC.default_position_update () in
  u.total_number_messages <- Some (Int32.of_int_exn nb_msgs) ;
  u.message_number <- Some (Int32.of_int_exn msg_number) ;
  u.request_id <- request_id ;
  u.symbol <- symbol ;
  u.exchange <- Some !my_exchange ;
  u.trade_account <- Some (trade_accountf ~userid ~username) ;
  u.average_price <- avgEntryPrice ;
  u.quantity <- Option.map currentQty ~f:Int.to_float ;
  write_message w `position_update DTC.gen_position_update u

let write_balance_update ?request_id ?(msg_number=1) ?(nb_msgs=1) ~userid ~username w m =
  let open RespObj in
  let u = DTC.default_account_balance_update () in
  u.request_id <- request_id ;
  u.unsolicited <- Some (Option.is_none request_id) ;
  u.total_number_messages <- Some (Int32.of_int_exn nb_msgs) ;
  u.message_number <- Some (Int32.of_int_exn msg_number) ;
  u.account_currency <- Some "mXBT" ;
  u.cash_balance <- Some (Int64.(to_float @@ int64_exn m "walletBalance") /. 1e5) ;
  u.balance_available_for_new_positions <-
    Some (Int64.(to_float @@ int64_exn m "availableMargin") /. 1e5) ;
  u.securities_value <-
    Some (Int64.(to_float @@ int64_exn m "marginBalance") /. 1e5) ;
  u.margin_requirement <- (Int64.(
      Some (to_float (int64_exn m "initMargin" +
                      int64_exn m "maintMargin" +
                      int64_exn m "sessionMargin") /. 1e5))) ;
  u.trade_account <- Some (trade_accountf ~userid ~username) ;
  write_message w `account_balance_update DTC.gen_account_balance_update u

let write_trade_account ?request_id ~message_number ~total_number_messages ~trade_account w =
  let resp = DTC.default_trade_account_response () in
  resp.total_number_messages <- Some (Int32.of_int_exn total_number_messages) ;
  resp.message_number <- Some (Int32.of_int_exn message_number) ;
  resp.trade_account <- Some trade_account ;
  resp.request_id <- request_id ;
  write_message w `trade_account_response DTC.gen_trade_account_response resp

let write_trade_accounts ?request_id { Connection.addr ; w ; usernames } =
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
    { REST.ApiKey.id; secret; permissions; enabled; userId } =
  if enabled then begin
    Int.Table.set apikeys ~key:userId ~data:{ key = id ; secret };
    let sc_account = List.find_map permissions ~f:begin function
      | Dtc username -> Some username
      | Perm _ -> None
      end
    in
    Option.iter sc_account ~f:begin fun sc_account ->
      Int.Table.set usernames userId sc_account ;
      String.Table.set accounts sc_account userId ;
      Log.debug log_bitmex "[%s] Add key for %s:%d" addr sc_account userId
    end
  end

let rec populate_api_keys
    ({ Connection.addr ; apikeys ; usernames ; accounts ; need_resubscribe ;
        to_bitmex_w ; to_client_r } as conn) entries =
  Log.debug log_bitmex "[%s] Found %d api keys entries"
    addr (List.length entries) ;
  let old_apikeys = Int.Set.of_hashtbl_keys apikeys in
  Int.Table.clear apikeys;
  Int.Table.clear usernames;
  String.Table.clear accounts;
  List.iter entries ~f:(add_api_keys conn);
  let new_apikeys = Int.Set.of_hashtbl_keys apikeys in
  if not @@ Int.Set.equal old_apikeys new_apikeys then
    write_trade_accounts conn ;
  let keys_to_delete =
    if need_resubscribe then Int.Set.empty else
    Int.Set.diff old_apikeys new_apikeys in
  let keys_to_add =
    if need_resubscribe then new_apikeys
    else Int.Set.diff new_apikeys old_apikeys in
  Log.debug log_bitmex "[%s] add %d key(s), delete %d key(s)" addr
    (Int.Set.length keys_to_add) (Int.Set.length keys_to_delete);
  Deferred.List.iter
    (Int.Set.to_list keys_to_delete) ~how:`Sequential ~f:begin fun id ->
    Log.debug log_bitmex "[%s] Unsubscribe %d" addr id;
    Pipe.write to_bitmex_w @@ Unsubscribe { addr ; id } >>= fun () ->
    Pipe.read to_client_r >>| function
    | `Ok Unsubscribe { id=id'; addr=addr' } when id = id' && addr = addr' -> ()
    | _ -> failwithf "Unsubscribe %s %d failed" addr id ()
  end >>= fun () ->
  Deferred.List.iter
    (Int.Set.to_list keys_to_add) ~how:`Sequential ~f:begin fun id ->
    Log.debug log_bitmex "[%s] Subscribe %d" addr id;
    Pipe.write to_bitmex_w @@ Subscribe { addr ; id } >>= fun () ->
    Pipe.read to_client_r >>| function
    | `Ok Subscribe { id=id'; addr=addr' } when id = id' && addr = addr' -> ()
    | _ -> failwithf "Subscribe %s %d failed" addr id ()
  end >>| fun () ->
  conn.need_resubscribe <- false

let populate_api_keys ({ Connection.key ; secret } as conn) =
  REST.ApiKey.dtc ~log:log_bitmex ~key ~secret ~testnet:!use_testnet () |>
  Deferred.Or_error.bind ~f:begin fun (_response, entries) ->
    Monitor.try_with_or_error (fun () -> populate_api_keys conn entries)
  end

let with_userid { Connection.addr ; key ; secret ; apikeys ; usernames } ~userid ~f =
  Int.Table.iteri apikeys ~f:begin fun ~key:userid' ~data:{ key; secret } ->
    if userid = userid' then
      Int.Table.find usernames userid |> Option.iter ~f:begin fun username ->
        try f ~addr ~userid ~username ~key ~secret with
          exn -> Log.error log_bitmex "%s" @@ Exn.to_string exn
      end
  end

let process_orders { Connection.addr ; w ; order } partial_iv action orders =
  List.iter orders ~f:begin fun o_json ->
    let o = RespObj.of_json o_json in
    let oid_string = RespObj.string_exn o "orderID" in
    let oid = Uuid.of_string oid_string in
    let account = RespObj.int_exn o "account" in
    let order = Int.Table.find_or_add ~default:Uuid.Table.create order account in
    match action with
    | WS.Response.Update.Delete ->
        Uuid.Table.remove order oid;
        Log.debug log_bitmex "<- [%s] order delete" addr
    | Insert | Partial ->
        Uuid.Table.set order ~key:oid ~data:o;
        let symbol = RespObj.string_exn o "symbol" in
        let side = RespObj.string_exn o "side" in
        let ordType = RespObj.string_exn o "ordType" in
        Log.debug log_bitmex "<- [%s] order insert/partial %s %d %s %s %s"
          addr oid_string account symbol side ordType
    | Update ->
        if Ivar.is_full partial_iv then begin
          let data = match Uuid.Table.find order oid with
          | None -> o
          | Some old_o -> RespObj.merge old_o o
          in
          Uuid.Table.set order ~key:oid ~data;
          let symbol = RespObj.string_exn data "symbol" in
          let side = RespObj.string_exn data "side" in
          let ordType = RespObj.string_exn data "ordType" in
          Log.debug log_bitmex "<- [%s] order update %s %d %s %s %s"
            addr oid_string account symbol side ordType
        end
  end ;
  if action = Partial then Ivar.fill_if_empty partial_iv ()

let process_margins ({ Connection.addr ; w ; margin } as c) partial_iv action margins =
  List.iteri margins ~f:begin fun i m_json ->
    let m = RespObj.of_json m_json in
    let userid = RespObj.int_exn m "account" in
    let currency = RespObj.string_exn m "currency" in
    match action with
    | WS.Response.Update.Delete ->
        Log.debug log_bitmex "<- [%s] margin delete" addr ;
        IS.Table.remove margin (userid, currency)
    | Insert | Partial ->
        Log.debug log_bitmex "<- [%s] margin insert/partial" addr ;
        IS.Table.set margin ~key:(userid, currency) ~data:m;
        with_userid c ~userid ~f:begin fun ~addr:_ ~userid ~username ~key:_ ~secret:_ ->
          write_balance_update ~username ~userid w m
        end
    | Update ->
        Log.debug log_bitmex "<- [%s] margin update" addr ;
        if Ivar.is_full partial_iv then begin
          let m = match IS.Table.find margin (userid, currency) with
          | None -> m
          | Some old_m -> RespObj.merge old_m m
          in
          IS.Table.set margin ~key:(userid, currency) ~data:m;
          with_userid c ~userid ~f:begin fun ~addr:_ ~userid ~username ~key:_ ~secret:_ ->
            write_balance_update ~username ~userid w m
          end
        end
  end ;
  if action = Partial then Ivar.fill_if_empty partial_iv ()

let process_positions ({ Connection.addr ; w ; position } as c) partial_iv action positions =
  List.iter positions ~f:begin fun p_json ->
    let p = RespObj.of_json p_json in
    let userid = RespObj.int_exn p "account" in
    let s = RespObj.string_exn p "symbol" in
    match action with
    | WS.Response.Update.Delete ->
        IS.Table.remove position (userid, s);
        Log.debug log_bitmex "<- [%s] position delete" addr
    | Insert | Partial ->
        IS.Table.set position ~key:(userid, s) ~data:p;
        if RespObj.bool_exn p "isOpen" then begin
          Log.debug log_bitmex "<- [%s] position insert/partial" addr ;
          with_userid c ~userid ~f:begin fun ~addr:_ ~userid ~username ~key:_ ~secret:_ ->
            write_position_update ~userid ~username w p;
            Log.debug log_bitmex "-> [%s] position update (%s:%d)" addr username userid
          end
        end
    | Update ->
        if Ivar.is_full partial_iv then begin
          let old_p, p = match IS.Table.find position (userid, s) with
          | None -> None, p
          | Some old_p -> Some old_p, RespObj.merge old_p p
          in
          IS.Table.set position ~key:(userid, s) ~data:p;
          match old_p with
          | Some old_p when RespObj.bool_exn old_p "isOpen" ->
              Log.debug log_dtc "<- [%s] position update" addr ;
              with_userid c ~userid ~f:begin fun ~addr:_ ~userid ~username ~key:_ ~secret:_ ->
                write_position_update ~userid ~username w p ;
                Log.debug log_bitmex "-> [%s] position update (%s:%d)" addr username userid;
              end
          | _ -> ()
        end
  end ;
  if action = Partial then Ivar.fill_if_empty partial_iv ()

let process_execs ({ Connection.addr ; w} as c) action execs =
  let iter_f e_json =
    let e = RespObj.of_json e_json in
    let userid = RespObj.int_exn e "account" in
    let symbol = RespObj.string_exn e "symbol" in
    match action with
    | WS.Response.Update.Insert ->
        Log.debug log_bitmex "<- [%s] exec %s" addr symbol;
        with_userid c ~userid ~f:begin fun ~addr:_ ~userid ~username ~key:_ ~secret:_ ->
          write_order_update ~userid ~username w e
        end
    | _ -> ()
  in
  List.iter execs ~f:iter_f

let client_ws ({ Connection.addr; w; ws_r; key; secret; order; margin; position } as c) =
  let order_iv = Ivar.create () in
  let margin_iv = Ivar.create () in
  let position_iv = Ivar.create () in

  let on_update { Connection.userid ; update={ WS.Response.Update.table; action; data } } =
    match table, action, data with
    | "order", action, orders -> process_orders c order_iv action orders ;
    | "margin", action, margins -> process_margins c margin_iv action margins ;
    | "position", action, positions -> process_positions c position_iv action positions ;
    | "execution", action, execs -> process_execs c action execs
    | table, _, _ -> Log.error log_bitmex "Unknown table %s" table
  in
  let start = populate_api_keys c in
  Clock_ns.every
    ~continue_on_error:true
    ~start:(Deferred.ignore start)
    ~stop:(Writer.close_started w)
    Time_ns.Span.(of_int_sec 60)
    begin fun () -> don't_wait_for begin
        populate_api_keys c >>| function
        | Ok () -> ()
        | Error err -> Log.error log_bitmex "%s" @@ Error.to_string_hum err
      end
    end ;
  don't_wait_for @@ Monitor.handle_errors
    (fun () -> Pipe.iter_without_pushback ~continue_on_error:true ws_r ~f:on_update)
    (fun exn -> Log.error log_bitmex "%s" @@ Exn.to_string exn);
  start

exception No_such_order

let new_client_accepted, new_client_accepted_w = Pipe.create ()
let client_deleted, client_deleted_w = Pipe.create ()

let encoding_request addr w req =
  Log.debug log_dtc "<- [%s] Encoding Request" addr ;
  Dtc_pb.Encoding.(to_string (Response { version = 7 ; encoding = Protobuf })) |>
  Writer.write w ;
  Log.debug log_dtc "-> [%s] Encoding Response" addr

let accept_logon_request addr w req conn send_secdefs =
  let hb_span =
    Option.value_map req.DTC.Logon_request.heartbeat_interval_in_seconds
      ~default:(Time_ns.Span.of_int_sec 10)
      ~f:(fun span -> Time_ns.Span.of_int_sec (Int32.to_int_exn span)) in
  let r = DTC.default_logon_response () in
  r.protocol_version <- Some 7l ;
  r.server_name <- Some "BitMEX" ;
  r.result <- Some `logon_success ;
  r.result_text <- Some "Welcome to BitMEX DTC Server for Sierra Chart" ;
  r.symbol_exchange_delimiter <- Some "-" ;
  r.security_definitions_supported <- Some true ;
  r.market_data_supported <- Some true ;
  r.historical_price_data_supported <- Some false ;
  r.market_depth_is_supported <- Some true ;
  r.market_depth_updates_best_bid_and_ask <- Some true ;
  r.trading_is_supported <- Some true ;
  r.order_cancel_replace_supported <- Some true ;
  r.ocoorders_supported <- Some false ;
  r.bracket_orders_supported <- Some false ;

  send_heartbeat conn hb_span ;
  write_message w `logon_response DTC.gen_logon_response r ;

  Log.debug log_dtc "-> [%s] Logon Response" addr ;
  let on_instrument { Instrument.secdef } =
    secdef.request_id <- Some 110_000_000l ;
    secdef.is_final_message <- Some true ;
    write_message w `security_definition_response
      DTC.gen_security_definition_response secdef
  in
  if send_secdefs then Instrument.iter ~f:on_instrument

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
  match req.username, req.password with
  | Some key, Some secret ->
      let conn = Connection.create ~addr ~w ~key ~secret ~send_secdefs in
      Connection.set ~key:addr ~data:conn;
      don't_wait_for begin
        Pipe.write new_client_accepted_w conn >>= fun () ->
        client_ws conn >>| function
        | Ok () -> accept_logon_request addr w req conn send_secdefs
        | Error err ->
            Log.error log_bitmex "%s" @@ Error.to_string_hum err ;
            reject_logon_request addr w "Credentials rejected by BitMEX"
      end
  | _ ->
      reject_logon_request addr w "Username and/or Password not set"

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
    if !my_exchange <> exchange && not Instrument.(is_index symbol) then
      security_definition_reject addr w id "No such symbol %s %s" symbol exchange
    else begin
      match Instrument.find symbol with
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
    { Instrument.instrObj; last_trade_price;
      last_trade_size; last_trade_ts; last_quote_ts } =
  let open RespObj in
  if Instrument.is_index symbol then begin
    let snap = DTC.default_market_data_snapshot () in
    snap.symbol_id <- id ;
    snap.session_settlement_price <- float instrObj "prevPrice24h" ;
    snap.last_trade_price <- float instrObj "lastPrice" ;
    snap.last_trade_date_time <-
      string instrObj "timestamp" |>
      Option.map ~f:(Fn.compose float_of_ts Time_ns.of_string) ;
    write_message w `market_data_snapshot DTC.gen_market_data_snapshot snap
  end
  else begin
    let { Quote.bidPrice; bidSize; askPrice; askSize } = Quotes.find_exn symbol in
    let open Option in
    let snap = DTC.default_market_data_snapshot () in
    snap.session_settlement_price <-
      Some (value ~default:Float.max_finite_value (float instrObj "indicativeSettlePrice")) ;
    snap.session_high_price <-
      Some (value ~default:Float.max_finite_value @@ float instrObj "highPrice") ;
    snap.session_low_price <-
      Some (value ~default:Float.max_finite_value @@ float instrObj "lowPrice") ;
    snap.session_volume <-
      Some (value_map (int64 instrObj "volume") ~default:Float.max_finite_value ~f:Int64.to_float) ;
    snap.open_interest <-
      Some (value_map (int64 instrObj "openInterest") ~default:0xffffffffl ~f:Int64.to_int32_exn) ;
    snap.bid_price <- bidPrice ;
    snap.bid_quantity <- Option.(map bidSize ~f:Float.of_int) ;
    snap.ask_price <- askPrice ;
    snap.ask_quantity <- Option.(map askSize ~f:Float.of_int) ;
    snap.last_trade_price <- Some last_trade_price ;
    snap.last_trade_volume <- Some (Int.to_float last_trade_size) ;
    snap.last_trade_date_time <- Some (float_of_ts last_trade_ts) ;
    snap.bid_ask_date_time <- Some (float_of_ts last_quote_ts) ;
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
    when exchange <> !my_exchange && Instrument.(is_index symbol) ->
    reject_market_data_request ?id addr w "No such exchange %s" exchange
  | _, id, Some symbol, _ when not (Instrument.mem symbol) ->
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
    let instr = Instrument.find_exn symbol in
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
        let instr = Instrument.find_exn symbol in
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
  | _, id, Some symbol, _ when not (Instrument.mem symbol) ->
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

let order_is_open o : DTC.order_status_enum option =
  match RespObj.string_exn o "ordStatus" |> OrdStatus.of_string with
  | New -> Some `order_status_open
  | PartiallyFilled -> Some `order_status_partially_filled
  | PendingCancel -> Some `order_status_pending_cancel
  | PendingReplace -> Some `order_status_pending_cancel_replace
  | _ -> None

let get_open_orders ?user_id ?order_id order_table =
  match user_id, order_id with
  | None, None ->
      Int.Table.fold order_table ~init:[] ~f:begin fun ~key:uid ~data:orders a ->
        Uuid.Table.fold orders ~init:a ~f:begin fun ~key:oid ~data:o a ->
          match order_is_open o with
          | Some status -> (status, o) :: a
          | None -> a
        end
      end
  | Some user_id, None ->
      begin match Int.Table.find order_table user_id with
      | None -> []
      | Some table ->
          Uuid.Table.fold table ~init:[] ~f:begin fun ~key:uid ~data:o a ->
            match order_is_open o with
            | Some status -> (status, o) :: a
            | None -> a
          end
      end
  | Some user_id, Some order_id ->
      begin match Int.Table.find order_table user_id with
      | None -> []
      | Some table -> begin match Uuid.Table.find table order_id with
        | None -> raise No_such_order
        | Some o ->
            match order_is_open o with None -> [] | Some status -> [status, o]
        end
      end
  | None, Some order_id ->
      begin match Order.find order_table order_id with
      | None -> raise No_such_order
      | Some (_uuid, uid, o) ->
          match order_is_open o with Some status -> [status, o] | None -> []
      end

let write_empty_order_update ?request_id w =
  let u = DTC.default_order_update () in
  u.total_num_messages <- Some 1l ;
  u.message_number <- Some 1l ;
  u.request_id <- request_id ;
  u.no_orders <- Some true ;
  u.order_update_reason <- Some `open_orders_request_response ;
  write_message w `order_update DTC.gen_order_update u

let write_open_order_update ?request_id ~msg_number ~nb_msgs ~status ~conn ~w o =
  let userid = RespObj.int_exn o "account" in
  with_userid conn ~userid ~f:begin fun ~addr:_ ~userid ~username ~key:_ ~secret:_ ->
    let status_reason = status, `open_orders_request_response in
    write_order_update ?request_id ~nb_msgs ~msg_number
      ~userid ~username ~status_reason w o
  end

let reject_open_orders_request ?request_id w k =
  let rej = DTC.default_open_orders_reject () in
  rej.request_id <- request_id ;
  Printf.ksprintf begin fun reject_text ->
    rej.reject_text <- Some reject_text ;
    write_message w `open_orders_reject DTC.gen_open_orders_reject rej
  end k

let open_orders_request addr w msg =
  let conn = Connection.find_exn addr in
  Log.debug log_dtc "<- [%s] Open Orders Request" addr ;
  let req = DTC.parse_open_orders_request msg in
  let trade_account = Option.value ~default:"" req.trade_account in
  let user_id = Option.(cut_trade_account trade_account >>| snd) in
  let order_id = Option.map req.server_order_id ~f:Uuid.of_string in
  match get_open_orders ?user_id ?order_id conn.Connection.order with
  | [] ->
      write_empty_order_update ?request_id:req.request_id w ;
      Log.debug log_bitmex "[%s] -> Open Orders Response: No Open Orders" addr
  | oos ->
      let nb_msgs = List.length oos in
      List.iteri oos ~f:begin fun i (status, o) ->
        write_open_order_update ?request_id:req.request_id ~nb_msgs ~msg_number:(succ i)
          ~status ~conn ~w o
      end ;
      Log.debug log_bitmex "[%s] -> Open Orders Response: %d Open Orders" addr nb_msgs
  | exception No_such_order ->
      reject_open_orders_request ?request_id:req.request_id w "No such order" ;
      Log.error log_bitmex "[%s] -> Open Order Reject: No such order" addr

let write_current_position_update ?request_id ~msg_number ~nb_msgs ~conn ~w p =
  let userid = RespObj.int_exn p "account" in
  with_userid conn ~userid ~f:begin fun ~addr:_ ~userid ~username ~key:_ ~secret:_ ->
    write_position_update ?request_id ~nb_msgs ~msg_number ~userid ~username w p
  end

let write_no_positions ?trade_account ?request_id w =
  let u = DTC.default_position_update () in
  u.total_number_messages <- Some 1l ;
  u.message_number <- Some 1l ;
  u.request_id <- request_id ;
  u.no_positions <- Some true ;
  u.unsolicited <- Some false ;
  u.trade_account <- trade_account ;
  write_message w `position_update DTC.gen_position_update u

let get_open_positions position userid =
  IS.Table.fold position ~init:(0, [])
    ~f:begin fun ~key:(user_id', symbol) ~data ((nb_open_ps, open_ps) as acc) ->
      if RespObj.bool_exn data "isOpen" then
        match userid with
        | Some user_id when user_id = user_id' -> succ nb_open_ps, data :: open_ps
        | Some _user_id -> acc
        | None -> succ nb_open_ps, data :: open_ps
      else acc
    end

let current_positions_request addr w msg =
  let ({ Connection.addr ; position } as conn) = Connection.find_exn addr in
  Log.debug log_dtc "<- [%s] Current Positions Request" addr ;
  let req = DTC.parse_current_positions_request msg in
  let trade_account = Option.value ~default:"" req.trade_account in
  let userid = Option.map (cut_trade_account trade_account) ~f:snd in
  let nb_msgs, open_positions = get_open_positions position userid in
  List.iteri open_positions ~f:begin fun i p ->
    write_current_position_update ?request_id:req.request_id
      ~nb_msgs ~msg_number:(succ i) ~conn ~w p
  end ;
  if nb_msgs = 0 then
    write_no_positions ?trade_account:req.trade_account ?request_id:req.request_id w ;
  Log.debug log_dtc "-> [%s] Current Positions Request: %d positions" addr nb_msgs

let send_historical_order_fills_response ~userid ~username req addr w orders =
  let open RespObj in
  let resp = DTC.default_historical_order_fill_response () in
  let nb_msgs = List.length orders in
  resp.total_number_messages <- Some (Int32.of_int_exn nb_msgs) ;
  resp.request_id <- req.DTC.Historical_order_fills_request.request_id ;
  resp.trade_account <- Some (trade_accountf ~userid ~username) ;
  List.iteri orders ~f:begin fun i o ->
    let o = of_json o in
    let side = string_exn o "side" |> Side.of_string in
    resp.message_number <- Some Int32.(succ @@ of_int_exn i) ;
    resp.symbol <- Some (string_exn o "symbol") ;
    resp.exchange <- Some !my_exchange ;
    resp.server_order_id <- Some (string_exn o "orderID") ;
    resp.price <- Some (float_exn o "avgPx") ;
    resp.quantity <- Some Float.(of_int64 (int64_exn o "orderQty")) ;
    resp.date_time <-
      string o "transactTime" |>
      Option.map ~f:(Fn.compose seconds_int64_of_ts Time_ns.of_string) ;
    resp.buy_sell <- Some side ;
    resp.unique_execution_id <- Some (string_exn o "execID") ;
    write_message w `historical_order_fill_response
      DTC.gen_historical_order_fill_response resp
  end ;
  Log.debug log_dtc "-> [%s] Historical Order Fills Response %d" addr nb_msgs

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

let get_trade_history ?orderID ~key ~secret () =
  let filter = `Assoc begin List.filter_opt [
      Option.map orderID ~f:(fun id -> ("orderID", `String id)) ;
      Some ("execType", `String "Trade")
    ] end in
  REST.Execution.trade_history
    ~log:log_bitmex ~testnet:!use_testnet ~key ~secret ~filter ()

let send_trade_history { Connection.addr ; usernames } req w = function
| Ok (_resp, []) ->
    write_no_historical_order_fills req w
| Ok (_resp, userids_trades) ->
    send_historical_order_fills_response req addr w orders
| Error err ->
    Log.error log_bitmex "%s" @@ Error.to_string_hum err ;
    reject_historical_order_fills_request ?request_id:req.request_id w
      "Error fetching historical order fills from BitMEX"

let historical_order_fills_request addr w msg =
    let ({ Connection.apikeys } as conn) = Connection.find_exn addr in
    Log.debug log_dtc "<- [%s] Historical Order Fills Request" addr ;
    let req = DTC.parse_historical_order_fills_request msg in
    let trade_account = Option.value ~default:"" req.trade_account in
    match Option.map (cut_trade_account trade_account) ~f:snd with
    | None -> don't_wait_for begin
        Monitor.try_with_or_error begin fun () ->
          Deferred.List.fold (Int.Table.to_alist apikeys)
            ~init:(None, []) ~f:begin fun (_, a) (userid, { key; secret }) ->
            get_trade_history ?orderID:req.server_order_id ~key ~secret () >>| function
            | Error err -> raise (Error.to_exn err)
            | Ok (resp, trades) -> Some resp, (userid, trades) :: a
          end
        end >>| function
        | Error err ->
            reject_historical_order_fills_request ?request_id:req.request_id
              w "Error while fetching historical order fills"
        | Ok (None, _) -> assert false
        | Ok (Some resp, userids_trades) ->
            send_trade_history conn req w (Ok (resp, userids_trades))
      end
    | Some userid -> begin
        match Int.Table.find apikeys userid with
        | Some { key; secret } ->
            don't_wait_for begin
              get_trade_history ?orderID:req.server_order_id ~key ~secret () >>|
              send_trade_history conn req w
            end
        | None ->
            reject_historical_order_fills_request ?request_id:req.request_id w
              "No such account %s" trade_account
      end

let trade_accounts_request addr w msg =
  let req = DTC.parse_trade_accounts_request msg in
  let conn = Connection.find_exn addr in
  Log.debug log_dtc "<- [%s] Trade Accounts Request" addr ;
  write_trade_accounts ?request_id:req.request_id conn

let reject_account_balance_request ?request_id addr w k =
  let rej = DTC.default_account_balance_reject () in
  rej.request_id <- request_id ;
  Printf.ksprintf begin fun reject_text ->
    rej.reject_text <- Some reject_text ;
    write_message w `account_balance_reject  DTC.gen_account_balance_reject rej ;
    Log.debug log_dtc "-> [%s] Account Balance Reject" addr ;
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
  let { Connection.addr ; margin ; usernames } = Connection.find_exn addr in
  let req = DTC.parse_account_balance_request msg in
  let trade_account = Option.value ~default:"" req.trade_account in
  Log.debug log_dtc "<- [%s] Account Balance Request" addr ;
  let nb_msgs = IS.Table.length margin in
  if nb_msgs = 0 then
    write_no_balances req addr w
  else if trade_account = "" then
    IS.Table.fold margin ~init:1 ~f:begin fun ~key:(userid, currency) ~data:balance msg_number ->
      Int.Table.find_and_call usernames userid
        ~if_not_found:ignore
        ~if_found:begin fun usernames ->
          List.iter usernames ~f:begin fun username ->
            write_balance_update ?request_id:req.request_id ~msg_number ~nb_msgs
              ~username ~userid w balance ;
             Log.debug log_dtc "-> [%s] account balance: %s:%d" addr username userid
          end
        end;
      succ msg_number
    end |> ignore
  else begin
    let open Option in
    match cut_trade_account trade_account >>= fun (username, userid) ->
      IS.Table.find margin (userid, "XBt") >>| fun obj -> username, userid, obj
    with
    | Some (username, userid, balance) ->
        write_balance_update ?request_id:req.request_id ~msg_number:1 ~nb_msgs:1
          ~username ~userid w balance ;
        Log.debug log_dtc "-> [%s] account balance: %s:%d" addr username userid
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
  let stop_exec_inst = match ordType with
    | `order_type_market
    | `order_type_limit -> []
    | #OrderType.t -> [stop_exec_inst] in
  let displayQty, execInst = match timeInForce with
    | `tif_all_or_none -> Some 0, ExecInst.AllOrNone :: stop_exec_inst
    | #DTC.time_in_force_enum -> None, stop_exec_inst in
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
      end ~if_found:begin fun { key; secret } ->
        (* TODO: Enable selection of execInst *)
        don't_wait_for (submit_order w ~key ~secret req MarkPrice)
      end

let reject_cancel_replace_order (req : DTC.Cancel_replace_order.t) addr w k =
  let rej = DTC.default_order_update () in
  rej.total_num_messages <- Some 1l ;
  rej.message_number <- Some 1l ;
  rej.order_update_reason <- Some `order_cancel_replace_rejected ;
  rej.client_order_id <- req.client_order_id ;
  rej.server_order_id <- req.server_order_id ;
  rej.order_type <- req.order_type ;
  rej.price1 <- req.price1 ;
  rej.price2 <- req.price2 ;
  rej.order_quantity <- req.quantity ;
  rej.time_in_force <- req.time_in_force ;
  rej.good_till_date_time <- req.good_till_date_time ;
  Printf.ksprintf begin fun info_text ->
    rej.info_text <- Some info_text ;
    write_message w `order_update DTC.gen_order_update rej ;
    Log.debug log_dtc "-> [%s] Cancel Replace Rejected: %s" addr info_text
  end k

let amend_order addr w req key secret orderID ordType =
  let price1 = if req.DTC.Cancel_replace_order.price1_is_set = Some true then req.price1 else None in
  let price2 = if req.price2_is_set = Some true then req.price2 else None in
  let price, stopPx = OrderType.to_price_stopPx ?p1:price1 ?p2:price2 ordType in
  let amend = REST.Order.create_amend
    ?leavesQty:(Option.map req.quantity ~f:Float.to_int)
    ?price
    ?stopPx
    ~orderID () in
  REST.Order.amend_bulk ~log:log_bitmex ~testnet:!use_testnet ~key ~secret [amend] >>| function
  | Ok (_resp, body) ->
    Log.debug log_bitmex "<- %s" @@ Yojson.Safe.to_string body
  | Error err ->
    let err_str = Error.to_string_hum err in
    reject_cancel_replace_order req addr w "%s" err_str;
    Log.error log_bitmex "%s" err_str

let cancel_replace_order addr w msg =
  let ({ Connection.addr ; order } as conn) = Connection.find_exn addr in
  Log.debug log_dtc "<- [%s] Cancel Replace Order" addr ;
  let req = DTC.parse_cancel_replace_order msg in
  let order_type = Option.value ~default:`order_type_unset req.order_type in
  let time_in_force = Option.value ~default:`tif_unset req.time_in_force in
  if order_type <> `order_type_unset then
    reject_cancel_replace_order req addr w
      "Modification of ordType is not supported by BitMEX"
  else if time_in_force <> `tif_unset then
    reject_cancel_replace_order req addr w
      "Modification of timeInForce is not supported by BitMEX"
  else
  match Option.bind req.server_order_id ~f:begin fun orderID ->
          Order.find order (Uuid.of_string orderID)
        end with
  | None ->
      Log.error log_bitmex "Order Cancel Replace: Order Not Found" ;
      reject_cancel_replace_order req addr w "Order Not Found"
  | Some (orderID, userid, o) ->
      let ordType = RespObj.string_exn o "ordType" |> OrderType.of_string in
      with_userid conn ~userid ~f:begin fun ~addr:_ ~userid ~username ~key ~secret ->
        don't_wait_for (amend_order addr w req key secret orderID ordType)
      end

let reject_cancel_order (req : DTC.Cancel_order.t) addr w k =
  let rej = DTC.default_order_update () in
  rej.total_num_messages <- Some 1l ;
  rej.message_number <- Some 1l ;
  rej.order_update_reason <- Some `order_cancel_rejected ;
  rej.client_order_id <- req.client_order_id ;
  rej.server_order_id <- req.server_order_id ;
  Printf.ksprintf begin fun info_text ->
    rej.info_text <- Some info_text ;
    write_message w `order_update DTC.gen_order_update rej ;
    Log.debug log_dtc "-> [%s] Cancel Rejected: %s" addr info_text
  end k

let cancel_order req addr w key secret orderID =
  REST.Order.cancel
    ~log:log_bitmex ~testnet:!use_testnet ~key ~secret ~orderIDs:[orderID] () >>| function
  | Ok (_resp, body) ->
    Log.debug log_bitmex "<- %s" @@ Yojson.Safe.to_string body
  | Error err ->
    let err_str = Error.to_string_hum err in
    reject_cancel_order req addr w "%s" err_str;
    Log.error log_bitmex "%s" err_str

let cancel_order addr w msg =
    let ({ Connection.addr ; order } as conn) = Connection.find_exn addr in
    Log.debug log_dtc "<- [%s] Cancel Order" addr ;
    let req = DTC.parse_cancel_order msg in
    match Option.bind req.server_order_id ~f:begin fun orderID ->
      Order.find order (Uuid.of_string orderID)
    end with
    | None ->
        Log.error log_bitmex "Order Cancel: Order Not Found" ;
        reject_cancel_order req addr w "Order Not Found"
    | Some (orderID, userid, o) ->
      with_userid conn ~userid ~f:begin fun ~addr:_ ~userid ~username ~key ~secret ->
        don't_wait_for @@ cancel_order req addr w key secret orderID
      end

let dtcserver ~server ~port =
  let server_fun addr r w =
    don't_wait_for begin
      Condition.wait ws_feed_connected >>= fun () ->
      Deferred.all_unit [Writer.close w ; Reader.close r]
    end ;
    let addr = Socket.Address.Inet.to_string addr in
    (* So that process does not allocate all the time. *)
    let rec handle_chunk consumed buf ~pos ~len =
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
          handle_chunk (consumed + msglen) buf (pos + msglen) (len - msglen)
        end
    in
    let on_connection_io_error exn =
      Connection.remove addr ;
      Log.error log_dtc "on_connection_io_error (%s): %s" addr Exn.(to_string exn)
    in
    let cleanup () =
      Log.info log_dtc "client %s disconnected" addr ;
      begin match Connection.find addr with
      | None -> Deferred.unit
      | Some conn ->
          Connection.purge conn ;
          Pipe.write client_deleted_w conn
      end >>= fun () ->
      Connection.remove addr ;
      Deferred.all_unit [Writer.close w; Reader.close r]
    in
    Deferred.ignore @@ Monitor.protect ~finally:cleanup begin fun () ->
      Monitor.detach_and_iter_errors Writer.(monitor w) ~f:on_connection_io_error;
      Reader.(read_one_chunk_at_a_time r ~handle_chunk:(handle_chunk 0))
    end
  in
  let on_handler_error_f addr exn =
    Log.error log_dtc "on_handler_error (%s): %s"
      Socket.Address.(to_string addr) Exn.(to_string exn)
  in
  Conduit_async.serve
    ~on_handler_error:(`Call on_handler_error_f)
    server (Tcp.on_port port) server_fun

let update_trade { Trade.symbol; timestamp; price; size; side } =
  Log.debug log_bitmex "trade %s %s %f %d" symbol (Side.show side) price size;
  match side, Instrument.find symbol with
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
      | `buy -> `at_bid
      | `sell -> `at_ask
      | `buy_sell_unset -> `bid_ask_unset in
    let u = DTC.default_market_data_update_trade () in
    u.at_bid_or_ask <- Some at_bid_or_ask ;
    u.price <- Some price ;
    u.volume <- Some (Int.to_float size) ;
    u.date_time <- Some (float_of_ts timestamp) ;
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
  | Update, "instrument", instrs ->
    if Ivar.is_full Instrument.initialized then
      List.iter instrs ~f:Instrument.update
  | Delete, "instrument", instrs ->
    if Ivar.is_full Instrument.initialized then
      List.iter instrs ~f:Instrument.delete
  | _, "instrument", instrs ->
    List.iter instrs ~f:Instrument.insert ;
    Ivar.fill_if_empty Instrument.initialized ()
  | _, "orderBookL2", depths ->
    let depths = List.map depths ~f:OrderBook.L2.of_yojson in
    let depths = List.group depths
        ~break:(fun { symbol } { symbol=symbol' } -> symbol <> symbol')
    in
    don't_wait_for begin
      Ivar.read Instrument.initialized >>| fun () ->
      List.iter depths ~f:begin function
        | [] -> ()
        | h::t as ds ->
          Log.debug log_bitmex "depth update %s" h.symbol;
          List.iter ds ~f:(Books.update action)
      end;
      Ivar.fill_if_empty Books.initialized ()
    end
  | _, "trade", trades ->
    let open Trade in
    don't_wait_for begin
      Ivar.read Instrument.initialized >>| fun () ->
      List.iter trades ~f:(Fn.compose update_trade Trade.of_yojson)
    end
  | _, "quote", quotes ->
    List.iter quotes ~f:(Fn.compose Quotes.update Quote.of_yojson) ;
    Ivar.fill_if_empty Quotes.initialized ()
  | _, table, json ->
    Log.error log_bitmex "Unknown/ignored BitMEX DB table %s or wrong json %s"
      table Yojson.Safe.(to_string @@ `List json)

let subscribe_topics ?(topic="") ~id ~topics =
  let open Bmex_ws in
  let payload =
    Request.(subscribe (List.map topics ~f:Sub.create) |> to_yojson) in
  Bmex_ws.MD.message id topic payload

let stream_id ~uuid ~addr ~id =
  uuid ^ "|" ^ addr ^ "|" ^ Int.to_string id

let subscribe_client ?(topic="") ~uuid ~addr ~id () =
  let id = stream_id ~uuid ~addr ~id in
  Bmex_ws.MD.(subscribe ~id ~topic |> to_yojson)

let unsubscribe_client ?(topic="") ~uuid ~addr ~id () =
  let id = stream_id ~uuid ~addr ~id in
  Bmex_ws.MD.(unsubscribe ~id ~topic |> to_yojson)

let conn_userid_of_stream_id_exn stream_id =
  match String.split ~on:'|' stream_id with
  | [_; addr; userid] ->
      Option.map (Connection.find addr) ~f:begin fun conn ->
        conn, Int.of_string userid
      end
  | _ -> None

let bitmex_topics = Bmex_ws.Topic.[Instrument; Quote; OrderBookL2; Trade]
let clients_topics = Bmex_ws.Topic.[Order; Execution; Position; Margin]

let on_ws_msg to_ws_w my_uuid msg =
  let open Bmex_ws in
  match MD.of_yojson msg with
  | Subscribe _ -> ()
  | Unsubscribe { id ; topic } -> begin
      match conn_userid_of_stream_id_exn id with
      | None ->
          Log.info log_bitmex
            "Got Unsubscribe message from client not in table"
      | Some (conn, id) ->
          Pipe.write_without_pushback conn.to_client_w
            (Unsubscribe { addr = conn.addr ; id })
    end
  | Message { stream = { id ; topic } ; payload } -> begin
      match Response.of_yojson payload, conn_userid_of_stream_id_exn id with
      (* Server *)
      | Response.Welcome _, None ->
          Pipe.write_without_pushback to_ws_w @@
          MD.to_yojson @@ subscribe_topics my_uuid bitmex_topics
      | Error err, None ->
          Log.error log_bitmex "BitMEX: error %s" err
      | Response { subscribe = Some { topic ; symbol = Some sym } }, None ->
          Log.info log_bitmex
            "BitMEX: subscribed to %s:%s" (Topic.show topic) sym
      | Response { subscribe = Some { topic; symbol = None }}, None ->
          Log.info log_bitmex
            "BitMEX: subscribed to %s" (Topic.show topic)
      | Update update, None -> on_update update

      (* Clients *)
      | Welcome _, Some (conn, userid) ->
          with_userid conn ~userid ~f:begin fun ~addr:_ ~userid ~username ~key ~secret ->
            Pipe.write_without_pushback to_ws_w @@
            MD.(auth ~id ~topic:"" ~key ~secret |> to_yojson)
          end
      | Error err, Some (conn, userid) ->
          Log.error log_bitmex "[%s] %d: error %s" conn.addr userid err ;
      | Response { request = AuthKey _ ; success}, Some (conn, userid) ->
          Log.debug log_bitmex "[%s] %d: subscribe to topics" conn.addr userid;
          Pipe.write_without_pushback to_ws_w @@
            MD.to_yojson @@ subscribe_topics id clients_topics ;
          Pipe.write_without_pushback conn.to_client_w @@
          Subscribe { addr = conn.addr ; id = userid }
      | Response { subscribe = Some { topic = subscription } }, Some (conn, userid) ->
          Log.info log_bitmex "[%s] %d: subscribed to %s"
            conn.addr userid (Topic.show subscription)
      | Response _, Some (conn, userid) ->
          Log.error log_bitmex "[%s] %d: unexpected response %s"
            conn.addr userid (Yojson.Safe.to_string payload)
      | Update update, Some (conn, userid) ->
          let client_update = { Connection.userid ; update } in
          Pipe.write_without_pushback conn.ws_w client_update
      | _ -> ()
    end

let bitmex_ws
    ~instrs_initialized ~orderbook_initialized ~quotes_initialized =
  let open Bmex_ws in
  let to_ws, to_ws_w = Pipe.create () in
  let my_uuid = Uuid.(create () |> to_string) in
  let connected = Condition.create () in
  let rec resubscribe () =
    Condition.wait connected >>= fun () ->
    Condition.broadcast ws_feed_connected () ;
    Connection.iter ~f:(fun c -> c.need_resubscribe <- true);
    Pipe.write to_ws_w MD.(subscribe ~id:my_uuid ~topic:"" |> to_yojson) >>= fun () ->
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
      Deferred.List.iter usernames ~how:`Parallel ~f:begin fun (id, _) ->
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
    let instrs_initialized = Ivar.create () in
    let orderbook_initialized = Ivar.create () in
    let quotes_initialized = Ivar.create () in
    Log.info log_bitmex "WS feed starting";
    let bitmex_th =
      bitmex_ws ~instrs_initialized ~orderbook_initialized ~quotes_initialized
    in
    Deferred.List.iter ~how:`Parallel ~f:Ivar.read
      [instrs_initialized; orderbook_initialized; quotes_initialized] >>= fun () ->
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
  Command.Staged.async ~summary:"BitMEX bridge" spec main

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
