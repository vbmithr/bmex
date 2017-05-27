(*---------------------------------------------------------------------------
   Copyright (c) 2016 Vincent Bernardoff. All rights reserved.
   Distributed under the ISC license, see terms at the end of the file.
   %%NAME%% %%VERSION%%
  ---------------------------------------------------------------------------*)

(* DTC to BitMEX simple bridge *)

open Core
open Async
open Cohttp_async

open Dtc.Dtc

open Bs_devkit
open Bs_api.BMEX

module Util = struct
  module IS = struct
    module T = struct
      type t = Int.t * String.t [@@deriving sexp]
      let compare = compare
      let hash = Hashtbl.hash
    end
    include T
    include Hashable.Make (T)
    module Set = Set.Make (T)
  end

  let b64_of_uuid uuid_str =
    match Uuidm.of_string uuid_str with
    | None -> invalid_arg "Uuidm.of_string"
    | Some uuid -> Uuidm.to_bytes uuid |> B64.encode

  let uuid_of_b64 b64 =
    B64.decode b64 |> Uuidm.of_bytes |> function
    | None -> invalid_arg "uuid_of_b64"
    | Some uuid -> Uuidm.to_string uuid

  let trade_accountf ~userid ~username =
    username ^ ":" ^ Int.to_string userid

  let cut_trade_account = function
  | "" -> None
  | s -> match String.split s ~on:':' with
  | [username; id] -> Some (username, Int.of_string id)
  | _ -> invalid_arg "cut_trade_account"

  let rec loop_log_errors ?log f =
    let rec inner () =
      Monitor.try_with_or_error ~name:"loop_log_errors" f >>= function
      | Ok _ -> Deferred.unit
      | Error err ->
          Option.iter log ~f:(fun log -> Log.error log "run: %s" @@ Error.to_string_hum err);
          inner ()
    in inner ()

  let conduit_server ~tls ~crt_path ~key_path =
    if tls then
      Sys.file_exists crt_path >>= fun crt_exists ->
      Sys.file_exists key_path >>| fun key_exists ->
      match crt_exists, key_exists with
      | `Yes, `Yes -> `OpenSSL (`Crt_file_path crt_path, `Key_file_path key_path)
      | _ -> failwith "TLS crt/key file not found"
    else
    return `TCP
end
open Util

module Dtc_util = struct
  module Trading = struct
    module Order = struct
      module Submit = struct
        let reject cs m k =
          let reject reason =
            Trading.Order.Update.write
              ~nb_msgs:1
              ~msg_number:1
              ~trade_account:m.Trading.Order.Submit.trade_account
              ~status:`Rejected
              ~reason:UpdateReason.New_order_rejected
              ~cli_ord_id:m.cli_ord_id
              ~symbol:m.Trading.Order.Submit.symbol
              ~exchange:m.exchange
              ?ord_type:m.ord_type
              ?side:m.side
              ~p1:m.p1
              ~p2:m.p2
              ~order_qty:m.qty
              ?tif:m.tif
              ~good_till_ts:m.good_till_ts
              ~info_text:reason
              ~free_form_text:m.text
              cs
          in
          Printf.ksprintf reject k

        let update ?reason ~status cs m k =
          let update info_text =
            Trading.Order.Update.write
              ~nb_msgs:1
              ~msg_number:1
              ~trade_account:m.Trading.Order.Submit.trade_account
              ~status
              ?reason
              ~cli_ord_id:m.Trading.Order.Submit.cli_ord_id
              ?ord_type:m.ord_type
              ?side:m.side
              ?open_or_close:m.open_close
              ~p1:m.p1
              ~p2:m.p2
              ~order_qty:m.qty
              ?tif:m.tif
              ~good_till_ts:m.good_till_ts
              ~info_text
              ~free_form_text:m.text
              cs
          in
          Printf.ksprintf update k
      end
    end
  end
end

module Instrument = struct
  module RespObj = struct
    include RespObj
    let is_index symbol = symbol.[0] = '.'
    let to_secdef ~testnet t =
      let symbol = string_exn t "symbol" in
      let index = is_index symbol in
      let exchange =
        match index, testnet with
        | true, _ -> string_exn t "reference"
        | _, true -> "BMEXT"
        | _ -> "BMEX"
      in
      let tickSize = float_exn t "tickSize" in
      let expiration_date = Option.map (string t "expiry") ~f:(fun time ->
          Time_ns.(of_string time |>
                   to_int_ns_since_epoch |>
                   (fun t -> t / 1_000_000_000) |>
                   Int32.of_int_exn)) in
      let open SecurityDefinition in
      Response.create
        ~symbol
        ~exchange
        ~security_type:(if index then Index else Futures)
        ~descr:""
        ~min_price_increment:tickSize
        ~price_display_format:(price_display_format_of_ticksize tickSize)
        ~currency_value_per_increment:tickSize
        ~underlying_symbol:(string_exn t "underlyingSymbol")
        ~updates_bid_ask_only:false
        ~has_market_depth_data:(not index)
        ?expiration_date
        ()
  end

  type t = {
    mutable instrObj: RespObj.t;
    secdef: SecurityDefinition.Response.t;
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
end

let use_testnet = ref false
let base_uri = ref @@ Uri.of_string "https://www.bitmex.com"
let my_exchange = ref "BMEX"

let log_bitmex = Log.create ~level:`Error ~on_error:`Raise ~output:Log.Output.[stderr ()]
let log_dtc = Log.create ~level:`Error ~on_error:`Raise ~output:Log.Output.[stderr ()]
let log_ws = Log.create ~level:`Error ~on_error:`Raise ~output:Log.Output.[stderr ()]

let scratchbuf = Bigstring.create 4096
let ws_feed_connected : unit Condition.t = Condition.create ()

module Connection = struct
  type subscribe_msg =
    | Subscribe of { addr: string; id: int }
    | Unsubscribe of { addr: string; id: int }

  type apikey = {
    key: string;
    secret: Cstruct.t;
  }

  type client_update = {
    userid: int;
    update: Ws.update
  }

  type t = {
    addr: Socket.Address.Inet.t;
    addr_str: string;
    w: Writer.t;
    ws_r: client_update Pipe.Reader.t;
    ws_w: client_update Pipe.Writer.t;
    to_bitmex_r: subscribe_msg Pipe.Reader.t;
    to_bitmex_w: subscribe_msg Pipe.Writer.t;
    to_client_r: subscribe_msg Pipe.Reader.t;
    to_client_w: subscribe_msg Pipe.Writer.t;
    key: string;
    secret: Cstruct.t;
    position: RespObj.t IS.Table.t; (* indexed by account, symbol *)
    margin: RespObj.t IS.Table.t; (* indexed by account, currency *)
    order: RespObj.t Uuid.Table.t Int.Table.t; (* indexed by orderID *)
    mutable dropped: int;
    subs: int String.Table.t;
    subs_depth: int String.Table.t;
    send_secdefs: bool;

    apikeys : apikey Int.Table.t; (* indexed by account *)
    usernames : String.t list Int.Table.t; (* indexed by account *)
    accounts : Int.t list String.Table.t; (* indexed by SC username *)
    mutable need_resubscribe: bool;
    mutable ta_request_id: int32;
  }

  let create ~addr ~addr_str ~w ~key ~secret ~send_secdefs =
    let ws_r, ws_w = Pipe.create () in
    let to_bitmex_r, to_bitmex_w = Pipe.create () in
    let to_client_r, to_client_w = Pipe.create () in
    {
      addr ;
      addr_str ;
      w ;
      ws_r ;
      ws_w ;
      to_bitmex_r ;
      to_bitmex_w ;
      to_client_r ;
      to_client_w ;
      key ;
      secret = Cstruct.of_string secret ;
      position = IS.Table.create () ;
      margin = IS.Table.create () ;
      order = Int.Table.create () ;
      subs = String.Table.create () ;
      subs_depth = String.Table.create () ;
      send_secdefs ;
      apikeys = Int.Table.create () ;
      usernames = Int.Table.create () ;
      accounts = String.Table.create () ;

      dropped = 0 ;
      need_resubscribe = false ;
      ta_request_id = 0l ;
    }

  let purge { ws_r; to_bitmex_r; to_client_r } =
    Pipe.close_read ws_r;
    Pipe.close_read to_bitmex_r;
    Pipe.close_read to_client_r

  let active : t String.Table.t = String.Table.create ()
end

module Order = struct
  exception Found of int * RespObj.t
  let find order uuid = try
    Int.Table.iteri order ~f:begin fun ~key ~data ->
      match Uuid.Table.find data uuid with
      | Some o -> raise (Found (key, o))
      | None -> ()
    end;
    None
  with Found (account, o) -> Some (account, o)
end


type book_entry = {
  price: float;
  size: int;
}

type books = {
  bids: book_entry Int.Table.t;
  asks: book_entry Int.Table.t;
}

let mapify_ob table =
  let fold_f ~key:_ ~data:{ price; size } map =
    Float.Map.update map price ~f:(function
      | Some size' -> size + size'
      | None -> size
      )
  in
  Int.Table.fold table ~init:Float.Map.empty ~f:fold_f

let orderbooks : books String.Table.t = String.Table.create ()
let quotes : Quote.t String.Table.t = String.Table.create ()


let heartbeat { Connection.addr_str; w; dropped } ival =
  let open Logon.Heartbeat in
  let cs = Cstruct.create sizeof_cs in
  let rec loop () =
    Clock_ns.after @@ Time_ns.Span.of_int_sec ival >>= fun () ->
    Log.debug log_dtc "-> [%s] HB" addr_str;
    write ~dropped_msgs:dropped cs;
    Writer.write_cstruct w cs;
    loop ()
  in
  Monitor.try_with_or_error ~name:"heartbeat" loop >>| function
  | Error _ -> Log.error log_dtc "-/-> %s HB" addr_str
  | Ok _ -> ()

let write_order_update ?(nb_msgs=1) ?(msg_number=1) ~addr_str ~userid ~username w cs e =
  let invalid_arg' execType ordStatus =
    invalid_arg Printf.(sprintf "write_order_update: execType=%s, ordStatus=%s" execType ordStatus)
  in
  let execType = RespObj.(string_exn e "execType") in
  let ordStatus = RespObj.(string_exn e "ordStatus") in
  let status_reason_of_execType_ordStatus_exn () =
    if execType = ordStatus then
      match execType with
      | "New" -> `Open, UpdateReason.New_order_accepted
      | "PartiallyFilled" -> `Open, Partially_filled
      | "Filled" -> `Filled, Filled
      | "DoneForDay" -> `Open, General_order_update
      | "Canceled" -> `Canceled, Canceled
      | "PendingCancel" -> `Pending_cancel, General_order_update
      | "Stopped" -> `Open, General_order_update
      | "Rejected" -> `Rejected, New_order_rejected
      | "PendingNew" -> `Pending_open, General_order_update
      | "Expired" -> `Rejected, New_order_rejected
      | _ -> invalid_arg' execType ordStatus
    else
    match execType, ordStatus with
    | "Restated", _ ->
      (match ordStatus with
      | "New" -> `Open, General_order_update
      | "PartiallyFilled" -> `Open, General_order_update
      | "Filled" -> `Filled, General_order_update
      | "DoneForDay" -> `Open, General_order_update
      | "Canceled" -> `Canceled, General_order_update
      | "PendingCancel" -> `Pending_cancel, General_order_update
      | "Stopped" -> `Open, General_order_update
      | "Rejected" -> `Rejected, General_order_update
      | "PendingNew" -> `Pending_open, General_order_update
      | "Expired" -> `Rejected, General_order_update
      | _ -> invalid_arg' execType ordStatus
      )
    | "Trade", "Filled" -> `Filled, Filled
    | "Trade", "PartiallyFilled" -> `Filled, Partially_filled
    | "Replaced", "New" -> `Open, Cancel_replace_complete
    | "TriggeredOrActivatedBySystem", "New" -> `Open, New_order_accepted
    | "Funding", _ -> raise Exit
    | "Settlement", _ -> raise Exit
    | _ -> invalid_arg' execType ordStatus
  in
  match status_reason_of_execType_ordStatus_exn () with
  | exception Exit -> ()
  | exception Invalid_argument msg -> Log.error log_bitmex "Not sending order update for %s" msg
  | status, reason ->
    let price = RespObj.(float_or_null_exn ~default:Float.max_finite_value e "price") in
    let stopPx = RespObj.(float_or_null_exn ~default:Float.max_finite_value e "stopPx") in
    let ord_type = ord_type_of_string RespObj.(string_exn e "ordType") in
    let p1, p2 = p1_p2_of_bitmex ~ord_type ~stopPx ~price in
    Log.debug log_bitmex "-> [%s:%s:%d] OrderUpdate %s %s" addr_str username userid execType ordStatus;
    Trading.Order.Update.write
      ~nb_msgs
      ~msg_number
      ~symbol:RespObj.(string_exn e "symbol")
      ~exchange:!my_exchange
      ~cli_ord_id:RespObj.(string_exn e "clOrdID")
      ~srv_ord_id:RespObj.(string_exn e "orderID" |> b64_of_uuid)
      ~xch_ord_id:RespObj.(string_exn e "orderID" |> b64_of_uuid)
      ~ord_type
      ~status
      ~reason
      ?side:(RespObj.string_exn e "side" |> side_of_bmex)
      ?p1
      ?p2
      ~tif:(tif_of_string RespObj.(string_exn e "timeInForce"))
      ~order_qty:RespObj.(int64_exn e "orderQty" |> Int64.to_float)
      ~filled_qty:RespObj.(int64_exn e "cumQty" |> Int64.to_float)
      ~remaining_qty:RespObj.(int64_exn e "leavesQty" |> Int64.to_float)
      ?avg_fill_p:RespObj.(float e "avgPx")
      ?last_fill_p:RespObj.(float e "lastPx")
      ?last_fill_ts:RespObj.(string e "transactTime" |> Option.map ~f:Time_ns.of_string)
      ?last_fill_qty:RespObj.(int64 e "lastQty" |> Option.map ~f:Int64.to_float)
      ~last_fill_exec_id:RespObj.(string_exn e "execID" |> b64_of_uuid)
      ~trade_account:(trade_accountf ~userid ~username)
      ~free_form_text:RespObj.(string_exn e "text")
      cs;
    Writer.write_cstruct w cs

let write_position_update ~nb_msgs ~msg_number ~userid ~username w cs p =
  Trading.Position.Update.write
    ~nb_msgs
    ~msg_number
    ~symbol:RespObj.(string_exn p "symbol")
    ~exchange:!my_exchange
    ~trade_account:(trade_accountf ~userid ~username)
    ~p:RespObj.(float_or_null_exn ~default:0. p "avgEntryPrice")
    ~v:RespObj.(int64_exn p "currentQty" |> Int64.to_float)
    cs;
  Writer.write_cstruct w cs

let write_balance_update ?(msg_number=1) ?(nb_msgs=1) ?(unsolicited=true) ~username ~userid cs m =
  let open Account.Balance.Update in
  write
    ~nb_msgs
    ~msg_number
    ~currency:"mXBT"
    ~cash_balance:RespObj.(Int64.(to_float @@ int64_exn m "walletBalance") /. 1e5)
    ~balance_available:RespObj.(Int64.(to_float @@ int64_exn m "availableMargin") /. 1e5)
    ~securities_value:RespObj.(Int64.(to_float @@ int64_exn m "marginBalance") /. 1e5)
    ~margin_requirement:RespObj.(Int64.(
        to_float (int64_exn m "initMargin" +
                  int64_exn m "maintMargin" +
                  int64_exn m "sessionMargin") /. 1e5))
    ~trade_account:(trade_accountf ~userid ~username)
    cs

let write_trade_accounts c w =
  let open Account.List in
  let cs = Cstruct.of_bigarray ~off:0 ~len:Response.sizeof_cs scratchbuf in
  let trade_accounts = List.map (Int.Table.keys c.Connection.apikeys) ~f:begin fun userid ->
      match Int.Table.find c.usernames userid with
      | None -> []
      | Some usernames -> List.map usernames ~f:(fun username -> trade_accountf ~userid ~username)
    end |> List.concat
  in
  let nb_msgs = List.length trade_accounts in
  List.mapi trade_accounts ~f:begin fun i trade_account ->
    if c.ta_request_id <> 0l then begin
      Response.write ~request_id:c.ta_request_id ~msg_number:(succ i) ~nb_msgs ~trade_account cs;
      Writer.write_cstruct w cs
    end
  end |> ignore;
  if c.ta_request_id <> 0l then Log.debug log_dtc "-> [%s] TradeAccountResp: %d accounts" c.addr_str nb_msgs

let add_api_keys
    ({ addr_str; apikeys; usernames; accounts } : Connection.t)
    ({ id; secret; permissions; enabled; userId } : Rest.ApiKey.entry) =
  if enabled then begin
    Int.Table.set apikeys ~key:userId ~data:{ key = id ; secret = (Cstruct.of_string secret) };
    let usernames' = List.filter_map permissions ~f:begin function
      | `Dtc username -> Some username
      | `Perm _ -> None
      end
    in
    Int.Table.set usernames userId usernames';
    List.iter usernames' ~f:begin fun u ->
      String.Table.add_multi accounts u userId;
      Log.debug log_bitmex "[%s] Add key for %s:%d" addr_str u userId
    end
  end

let populate_api_keys ({ Connection.addr_str; w; to_bitmex_r; to_bitmex_w; to_client_r; to_client_w;
                         key; secret; apikeys; usernames; accounts; need_resubscribe } as c) =
  let rec inner_exn entries =
    Log.debug log_bitmex "[%s] Found %d api keys entries" addr_str @@ List.length entries;
    let old_apikeys = Int.Set.of_hashtbl_keys apikeys in
    Int.Table.clear apikeys;
    Int.Table.clear usernames;
    String.Table.clear accounts;
    List.iter entries ~f:(add_api_keys c);
    let new_apikeys = Int.Set.of_hashtbl_keys apikeys in
    if not @@ Int.Set.equal old_apikeys new_apikeys then write_trade_accounts c w;
    let keys_to_delete = if need_resubscribe then Int.Set.empty else Int.Set.diff old_apikeys new_apikeys in
    let keys_to_add = if need_resubscribe then new_apikeys else Int.Set.diff new_apikeys old_apikeys in
    Log.debug log_bitmex "[%s] add %d key(s), delete %d key(s)" addr_str
      (Int.Set.length keys_to_add) (Int.Set.length keys_to_delete);
    Deferred.List.iter (Int.Set.to_list keys_to_delete) ~how:`Sequential ~f:begin fun id ->
      Log.debug log_bitmex "[%s] Unsubscribe %d" addr_str id;
      Pipe.write to_bitmex_w @@ Unsubscribe { addr=addr_str; id } >>= fun () ->
      Pipe.read to_client_r >>| function
      | `Ok Unsubscribe { id=id'; addr=addr' } when id = id' && addr_str = addr' -> ()
      | _ -> failwithf "Unsubscribe %s %d failed" addr_str id ()
    end >>= fun () ->
    Deferred.List.iter (Int.Set.to_list keys_to_add) ~how:`Sequential ~f:begin fun id ->
      Log.debug log_bitmex "[%s] Subscribe %d" addr_str id;
      Pipe.write to_bitmex_w @@ Subscribe { addr=addr_str; id } >>= fun () ->
      Pipe.read to_client_r >>| function
      | `Ok Subscribe { id=id'; addr=addr' } when id = id' && addr_str = addr' -> ()
      | _ -> failwithf "Subscribe %s %d failed" addr_str id ()
    end >>| fun () ->
    c.need_resubscribe <- false
  in
  let inner entries = Monitor.try_with_or_error (fun () -> inner_exn entries) in
  Rest.ApiKey.dtc ~log:log_bitmex ~key ~secret ~testnet:!use_testnet () |>
  Deferred.Or_error.bind ~f:inner

let with_userid { Connection.addr_str ; key ; secret ; apikeys ; usernames } ~userid ~f =
  Int.Table.iteri apikeys ~f:begin fun ~key:userid' ~data:{ key; secret } ->
    if userid = userid' then
      Int.Table.find usernames userid |> Option.iter ~f:begin fun usernames ->
        List.iter usernames ~f:begin fun username ->
          try f ~addr_str ~userid ~username ~key ~secret with
            exn -> Log.error log_bitmex "%s" @@ Exn.to_string exn
        end
      end
  end

let client_ws ({ Connection.addr_str; w; ws_r; key; secret; order; margin; position } as c) =
  let position_update_cs = Cstruct.of_bigarray scratchbuf ~len:Trading.Position.Update.sizeof_cs in
  let order_update_cs = Cstruct.of_bigarray scratchbuf ~len:Trading.Order.Update.sizeof_cs in
  let balance_update_cs = Cstruct.of_bigarray scratchbuf ~len:Account.Balance.Update.sizeof_cs in
  let order_partial_done = Ivar.create () in
  let margin_partial_done = Ivar.create () in
  let position_partial_done = Ivar.create () in

  let process_orders ~action orders =
    List.iter orders ~f:begin fun o_json ->
      let o = RespObj.of_json o_json in
      let oid_string = RespObj.string_exn o "orderID" in
      let oid = Uuid.of_string oid_string in
      let account = RespObj.int_exn o "account" in
      let order = Int.Table.find_or_add ~default:Uuid.Table.create order account in
      match action with
      | "delete" ->
        Uuid.Table.remove order oid;
        Log.debug log_bitmex "<- [%s] order delete" addr_str
      | "insert" | "partial" ->
        Uuid.Table.set order ~key:oid ~data:o;
        let symbol = RespObj.string_exn o "symbol" in
        let side = RespObj.string_exn o "side" in
        let ordType = RespObj.string_exn o "ordType" in
        Log.debug log_bitmex "<- [%s] order insert/partial %s %d %s %s %s"
          addr_str oid_string account symbol side ordType
      | "update" ->
        if Ivar.is_full order_partial_done then begin
          let data = match Uuid.Table.find order oid with
          | None -> o
          | Some old_o -> RespObj.merge old_o o
          in
          Uuid.Table.set order ~key:oid ~data;
          let symbol = RespObj.string_exn data "symbol" in
          let side = RespObj.string_exn data "side" in
          let ordType = RespObj.string_exn data "ordType" in
          Log.debug log_bitmex "<- [%s] order update %s %d %s %s %s"
            addr_str oid_string account symbol side ordType
        end
      | _ -> invalid_arg @@ "process_orders: unknown action " ^ action
    end;
    if action = "partial" then Ivar.fill_if_empty order_partial_done ()
  in
  let process_margins ~action margins =
    List.iteri margins ~f:begin fun i m_json ->
      let m = RespObj.of_json m_json in
      let userid = RespObj.int_exn m "account" in
      let currency = RespObj.string_exn m "currency" in
      match action with
      | "delete" ->
        Log.debug log_bitmex "<- [%s] margin delete" addr_str;
        IS.Table.remove margin (userid, currency)
      | "insert" | "partial" ->
        Log.debug log_bitmex "<- [%s] margin insert/partial" addr_str;
        IS.Table.set margin ~key:(userid, currency) ~data:m;
        with_userid c ~userid ~f:begin fun ~addr_str:_ ~userid ~username ~key:_ ~secret:_ ->
          write_balance_update ~username ~userid balance_update_cs m;
          Writer.write_cstruct w balance_update_cs
        end
      | "update" ->
        Log.debug log_bitmex "<- [%s] margin update" addr_str;
        if Ivar.is_full margin_partial_done then begin
          let m = match IS.Table.find margin (userid, currency) with
          | None -> m
          | Some old_m -> RespObj.merge old_m m
          in
          IS.Table.set margin ~key:(userid, currency) ~data:m;
          with_userid c ~userid ~f:begin fun ~addr_str:_ ~userid ~username ~key:_ ~secret:_ ->
            write_balance_update ~username ~userid balance_update_cs m;
            Writer.write_cstruct w balance_update_cs
          end
        end
      | _ -> invalid_arg @@ "process_margins: unknown action " ^ action
    end;
    if action = "partial" then Ivar.fill_if_empty margin_partial_done ()
  in
  let process_positions ~action positions =
    List.iter positions ~f:begin fun p_json ->
      let p = RespObj.of_json p_json in
      let userid = RespObj.int_exn p "account" in
      let s = RespObj.string_exn p "symbol" in
      match action with
      | "delete" ->
        IS.Table.remove position (userid, s);
        Log.debug log_bitmex "<- [%s] position delete" addr_str
      | "insert" | "partial" ->
        IS.Table.set position ~key:(userid, s) ~data:p;
        if RespObj.bool_exn p "isOpen" then begin
          Log.debug log_bitmex "<- [%s] position insert/partial" addr_str;
          with_userid c ~userid ~f:begin fun ~addr_str:_ ~userid ~username ~key:_ ~secret:_ ->
            write_position_update ~nb_msgs:1 ~msg_number:1 ~userid ~username w position_update_cs p;
            Log.debug log_bitmex "-> [%s] position update (%s:%d)" addr_str username userid
          end
        end
      | "update" ->
        if Ivar.is_full position_partial_done then begin
          let old_p, p = match IS.Table.find position (userid, s) with
          | None -> None, p
          | Some old_p -> Some old_p, RespObj.merge old_p p
          in
          IS.Table.set position ~key:(userid, s) ~data:p;
          match old_p with
          | Some old_p when RespObj.bool_exn old_p "isOpen" ->
            Log.debug log_dtc "<- [%s] position update" addr_str;
            with_userid c ~userid ~f:begin fun ~addr_str:_ ~userid ~username ~key:_ ~secret:_ ->
              write_position_update ~nb_msgs:1 ~msg_number:1 ~userid ~username w position_update_cs p;
              Log.debug log_bitmex "-> [%s] position update (%s:%d)" addr_str username userid;
            end
          | _ -> ()
        end
      | _ -> invalid_arg @@ "process_positions: unknown action " ^ action
    end;
    if action = "partial" then Ivar.fill_if_empty position_partial_done ()
  in
  let process_execs ~action execs =
    let iter_f e_json =
      let e = RespObj.of_json e_json in
      let userid = RespObj.int_exn e "account" in
      let symbol = RespObj.string_exn e "symbol" in
      match action with
      | "insert" ->
        Log.debug log_bitmex "<- [%s] exec %s" addr_str symbol;
        with_userid c ~userid ~f:begin fun ~addr_str ~userid ~username ~key:_ ~secret:_ ->
          write_order_update ~addr_str ~userid ~username w order_update_cs e
        end
      | _ -> ()
    in
    List.iter execs ~f:iter_f
  in
  let on_update { Connection.userid; update={ Ws.table; action; data } } =
    let open Ws in
    (* Erase scratchbuf by security. *)
    Bigstring.set_tail_padded_fixed_string scratchbuf ~padding:'\x00' ~pos:0 ~len:(Bigstring.length scratchbuf) "";
    match table, action, data with
    | "order", action, orders -> process_orders ~action orders
    | "margin", action, margins -> process_margins ~action margins
    | "position", action, positions -> process_positions ~action positions
    | "execution", action, execs -> process_execs ~action execs
    | table, _, _ -> Log.error log_bitmex "Unknown table %s" table
  in
  let start = populate_api_keys c in
  if key <> "" then Clock_ns.every
      ~continue_on_error:true
      ~start:(Deferred.ignore start)
      ~stop:(Writer.close_started w)
      Time_ns.Span.(of_int_sec 60)
      (fun () -> don't_wait_for begin
           populate_api_keys c >>| function
           | Ok () -> ()
           | Error err -> Log.error log_bitmex "%s" @@ Error.to_string_hum err
         end
      );
  don't_wait_for @@ Monitor.handle_errors
    (fun () -> Pipe.iter_without_pushback ~continue_on_error:true ws_r ~f:on_update)
    (fun exn -> Log.error log_bitmex "%s" @@ Exn.to_string exn);
  start

exception No_such_order

let new_client_accepted, new_client_accepted_w = Pipe.create ()
let client_deleted, client_deleted_w = Pipe.create ()

let process addr w msg_cs scratchbuf =
  let addr_str = Socket.Address.Inet.to_string addr in
  (* Erase scratchbuf by security. *)
  Bigstring.set_tail_padded_fixed_string
    scratchbuf ~padding:'\x00' ~pos:0 ~len:(Bigstring.length scratchbuf) "";
  let msg = msg_of_enum Cstruct.LE.(get_uint16 msg_cs 2) in
  let conn = match msg with
  | None -> None
  | Some EncodingRequest -> None
  | Some LogonRequest -> None
  | Some msg -> begin
      match String.Table.find Connection.active addr_str with
      | None ->
        Log.error log_dtc "msg type %s and found no client record" (show_msg msg);
        failwith "internal error: no client record"
      | Some conn -> Some conn
    end
  in
  match msg with
  | Some EncodingRequest ->
    let open Encoding in
    Log.debug log_dtc "<- [%s] EncodingReq" addr_str;
    let response_cs = Cstruct.of_bigarray scratchbuf ~len:Response.sizeof_cs in
    Response.write response_cs;
    Writer.write_cstruct w response_cs;
    Log.debug log_dtc "-> [%s] EncodingResp" addr_str

  | Some LogonRequest ->
    let open Logon in
    let m = Request.read msg_cs in
    let response_cs = Cstruct.of_bigarray scratchbuf ~len:Response.sizeof_cs in
    Log.debug log_dtc "<- [%s] %s" addr_str (Request.show m);
    let send_secdefs = Int32.(bit_and m.integer_1 128l <> 0l) in

    let accept client ~trading_supported =
      let resp =
        Response.create
          ~server_name:"BitMEX"
          ~result:LogonStatus.Success
          ~result_text:"Welcome to BitMEX DTC Server for Sierra Chart"
          ~security_definitions_supported:true
          ~market_data_supported:true
          ~historical_price_data_supported:false
          ~market_depth_supported:true
          ~market_depth_updates_best_bid_and_ask:true
          ~trading_supported
          ~ocr_supported:true
          ~oco_supported:false
          ~bracket_orders_supported:false
          ()
      in
      don't_wait_for @@ heartbeat client m.Request.heartbeat_interval;
      Response.to_cstruct response_cs resp;
      Writer.write_cstruct w response_cs;
      Log.debug log_dtc "-> [%s] %s" addr_str (Response.show resp);
      let secdef_resp_cs = Cstruct.of_bigarray scratchbuf ~len:SecurityDefinition.Response.sizeof_cs in
      let on_instrument { Instrument.secdef } =
        SecurityDefinition.Response.to_cstruct secdef_resp_cs { secdef with final = true; request_id = 110_000_000l };
        Writer.write_cstruct w secdef_resp_cs
      in
      if send_secdefs then ignore @@ String.Table.iter Instrument.active ~f:on_instrument
    in
    let conn = Connection.create ~addr ~addr_str ~w
        ~key: m.username ~secret:m.general_text_data ~send_secdefs in
    String.Table.set Connection.active ~key:addr_str ~data:conn;
    don't_wait_for begin
      Pipe.write new_client_accepted_w conn >>= fun () ->
      client_ws conn >>| function
      | Ok () -> accept conn ~trading_supported:true
      | Error err ->
        Log.error log_bitmex "%s" @@ Error.to_string_hum err;
        accept conn ~trading_supported:false
    end

  | Some Heartbeat ->
    Option.iter conn ~f:begin fun { addr_str } ->
      Log.debug log_dtc "<- [%s] HB" addr_str
    end

  | Some SecurityDefinitionForSymbolRequest ->
    let open SecurityDefinition in
    let { Request.id; symbol; exchange } = Request.read msg_cs in
    let { Connection.addr_str } = Option.value_exn conn in
    Log.debug log_dtc "<- [%s] SeqDefReq %s-%s" addr_str symbol exchange;
    let reject_cs = Cstruct.of_bigarray scratchbuf ~len:Reject.sizeof_cs in
    let reject k = Printf.ksprintf begin fun msg ->
        Reject.write reject_cs ~request_id:id "%s" msg;
        Log.debug log_dtc "-> [%s] SeqDefRej %s-%s" addr_str symbol exchange;
        Writer.write_cstruct w reject_cs
      end k
    in
    if !my_exchange <> exchange && not Instrument.RespObj.(is_index symbol) then
      reject "No such symbol %s-%s" symbol exchange
    else begin match String.Table.find Instrument.active symbol with
    | None -> reject "No such symbol %s-%s" symbol exchange
    | Some { Instrument.secdef } ->
      let open Response in
      let secdef = { secdef with request_id = id; final = true } in
      Log.debug log_dtc "-> [%s] SeqDefResp %s-%s" addr_str symbol exchange;
      let secdef_resp_cs = Cstruct.of_bigarray scratchbuf ~len:sizeof_cs in
      Response.to_cstruct secdef_resp_cs secdef;
      Writer.write_cstruct w secdef_resp_cs
    end

  | Some MarketDataRequest ->
    let open MarketData in
    let { Request.action; symbol_id; symbol; exchange } = Request.read msg_cs in
    Log.debug log_dtc "<- [%s] MarketDataReq %s %s-%s" addr_str (RequestAction.show action) symbol exchange;
    let { Connection.addr_str; subs } = Option.value_exn conn in
    let r_cs = Cstruct.of_bigarray ~off:0 ~len:Reject.sizeof_cs scratchbuf in
    let reject k = Printf.ksprintf begin fun reason ->
        Reject.write r_cs ~symbol_id "%s" reason;
        Log.debug log_dtc "-> [%s] MarketDataRej %s-%s %s" addr_str symbol exchange reason;
        Writer.write_cstruct w r_cs
      end k
    in
    let snap_of_instr { Instrument.instrObj; last_trade_price; last_trade_size; last_trade_ts; last_quote_ts } =
      if action = Unsubscribe then String.Table.remove subs symbol;
      if action = Subscribe then String.Table.set subs symbol symbol_id;
      if Instrument.RespObj.is_index symbol then
        Snapshot.create
          ~symbol_id:symbol_id
          ?session_settlement_price:(RespObj.float instrObj "prevPrice24h")
          ?last_trade_p:(RespObj.float instrObj "lastPrice")
          ~last_trade_ts:(RespObj.string_exn instrObj "timestamp" |> Time_ns.of_string)
          ()
      else
      (* let open RespObj in *)
      let { Quote.bidPrice; bidSize; askPrice; askSize } = String.Table.find_exn quotes symbol in
      let open RespObj in
      Snapshot.create
        ~symbol_id
        ~session_settlement_price:Option.(value ~default:Float.max_finite_value (float instrObj "indicativeSettlePrice"))
        ~session_h:Option.(value ~default:Float.max_finite_value @@ float instrObj "highPrice")
        ~session_l:Option.(value ~default:Float.max_finite_value @@ float instrObj "lowPrice")
        ~session_v:Option.(value_map (int64 instrObj "volume") ~default:Float.max_finite_value ~f:Int64.to_float)
        ~open_interest:Option.(value_map (int64 instrObj "openInterest") ~default:0xffffffffl ~f:Int64.to_int32_exn)
        ?bid:bidPrice
        ?bid_qty:Option.(map bidSize ~f:Float.of_int)
        ?ask:askPrice
        ?ask_qty:Option.(map askSize ~f:Float.of_int)
        ~last_trade_p:last_trade_price
        ~last_trade_v:(Int.to_float last_trade_size)
        ~last_trade_ts
        ~bid_ask_ts:last_quote_ts
        ()
    in
    if !my_exchange <> exchange && not Instrument.RespObj.(is_index symbol) then
      reject "No such symbol %s-%s" symbol exchange
    else begin match String.Table.find Instrument.active symbol with
    | None -> reject "No such symbol %s-%s" symbol exchange
    | Some instr -> try
      let snap = snap_of_instr instr in
      let snap_cs = Cstruct.of_bigarray scratchbuf ~off:0 ~len:Snapshot.sizeof_cs in
      Snapshot.to_cstruct snap_cs snap;
      Log.debug log_dtc "-> [%s] MarketDataSnap %s-%s" addr_str symbol exchange;
      Writer.write_cstruct w snap_cs
    with
    | Not_found ->
      Log.info log_dtc "market data request: no quote found for %s" symbol;
      reject "No market data for symbol %s-%s" symbol exchange;
    | exn ->
      Log.error log_dtc "%s" Exn.(to_string exn);
      reject "No market data for symbol %s-%s" symbol exchange;
    end

  | Some MarketDepthRequest ->
    let open MarketDepth in
    let { Request.action; symbol_id; symbol; exchange; nb_levels } = Request.read msg_cs in
    let { Connection.addr_str; subs_depth } = Option.value_exn conn in
    Log.debug log_dtc "<- [%s] MarketDepthReq %s-%s %d" addr_str symbol exchange nb_levels;
    let r_cs = Cstruct.of_bigarray ~off:0 ~len:Reject.sizeof_cs scratchbuf in
    let reject k = Printf.ksprintf begin fun msg ->
        Reject.write r_cs ~symbol_id "%s" msg;
        Log.debug log_dtc "-> [%s] MarketDepthRej: %s" addr_str msg;
        Writer.write_cstruct w r_cs
      end k
    in
    let accept { bids; asks } =
      if action = Unsubscribe then String.Table.remove subs_depth symbol;
      if action = Subscribe then String.Table.set subs_depth symbol symbol_id;
      let snap_cs = Cstruct.of_bigarray scratchbuf ~off:0 ~len:Snapshot.sizeof_cs in
      let bids = mapify_ob bids in
      let asks = mapify_ob asks in
      if Float.Map.(is_empty bids && is_empty asks) then begin
        Snapshot.write snap_cs ~symbol_id ~p:0. ~v:0. ~lvl:0 ~first:true ~last:true;
        Writer.write_cstruct w snap_cs
      end
      else begin
        Float.Map.fold_right bids ~init:1 ~f:begin fun ~key:price ~data:size lvl ->
          Snapshot.write snap_cs
            ~symbol_id
            ~side:`Buy ~p:price ~v:Float.(of_int size) ~lvl
            ~first:(lvl = 1) ~last:false;
          Writer.write_cstruct w snap_cs;
          succ lvl
        end |> ignore;
        Float.Map.fold asks ~init:1 ~f:begin fun ~key:price ~data:size lvl ->
          Snapshot.write snap_cs
            ~symbol_id
            ~side:`Sell ~p:price ~v:Float.(of_int size) ~lvl
            ~first:(lvl = 1 && Float.Map.is_empty bids) ~last:false;
          Writer.write_cstruct w snap_cs;
          succ lvl
        end |> ignore;
        Snapshot.write snap_cs ~symbol_id ~p:0. ~v:0. ~lvl:0 ~first:false ~last:true;
        Writer.write_cstruct w snap_cs
      end
    in
    if Instrument.RespObj.is_index symbol then
      reject "%s is an index" symbol
    else if !my_exchange <> exchange then
      reject "No such symbol %s-%s" symbol exchange
    else if action <> Unsubscribe && not @@ String.Table.mem Instrument.active symbol then
      reject "No such symbol %s-%s" symbol exchange
    else begin match String.Table.find orderbooks symbol with
    | None ->
      Log.error log_dtc "MarketDepthReq: found no orderbooks for %s" symbol;
      reject "Found no orderbook for symbol %s-%s" symbol exchange
    | Some obs ->
      accept obs
    end

  | Some OpenOrdersRequest ->
    let open Trading.Order in
    let ({ Open.Request.id; order; trade_account } as m) = Open.Request.read msg_cs in
    let ({ Connection.addr_str; order=order_table } as c) = Option.value_exn conn in
    Log.debug log_dtc "<- [%s] OpenOrdersReq (%ld) (%s) (%s)" addr_str id m.order m.trade_account;
    let reject_cs = Cstruct.of_bigarray ~off:0 ~len:Open.Reject.sizeof_cs scratchbuf in
    let update_cs = Cstruct.of_bigarray ~off:0 ~len:Update.sizeof_cs scratchbuf in
    let send_order_update ~request_id ~msg_number ~nb_msgs (status, data) =
      let userid = RespObj.int_exn data "account" in
      let ord_type = ord_type_of_string RespObj.(string_exn data "ordType") in
      let price = RespObj.(float_or_null_exn ~default:Float.max_finite_value data "price") in
      let stopPx = RespObj.(float_or_null_exn ~default:Float.max_finite_value data "stopPx") in
      let p1, p2 = p1_p2_of_bitmex ~ord_type ~stopPx ~price in
      with_userid c ~userid ~f:begin fun ~addr_str:_ ~userid ~username ~key:_ ~secret:_ ->
        Update.write
          ~nb_msgs
          ~msg_number
          ~status
          ~reason:UpdateReason.Open_orders_request_response
          ~request_id
          ~symbol:RespObj.(string_exn data "symbol")
          ~exchange:!my_exchange
          ~cli_ord_id:RespObj.(string_exn data "clOrdID")
          ~srv_ord_id:RespObj.(string_exn data "orderID" |> b64_of_uuid)
          ~xch_ord_id:RespObj.(string_exn data "orderID" |> b64_of_uuid)
          ~ord_type:(ord_type_of_string RespObj.(string_exn data "ordType"))
          ?side:(RespObj.string_exn data "side" |> side_of_bmex)
          ?p1
          ?p2
          ~order_qty:RespObj.(int64_exn data "orderQty" |> Int64.to_float)
          ~filled_qty:RespObj.(int64_exn data "cumQty" |> Int64.to_float)
          ~remaining_qty:RespObj.(int64_exn data "leavesQty" |> Int64.to_float)
          ~tif:(tif_of_string RespObj.(string_exn data "timeInForce"))
          ~trade_account:(trade_accountf ~userid ~username)
          ~free_form_text:RespObj.(string_exn data "text")
          update_cs;
        Writer.write_cstruct w update_cs
      end
    in
    let send_no_open_orders ~request_id =
      Update.write ~nb_msgs:1 ~msg_number:1 ~request_id:id
        ~reason:UpdateReason.Open_orders_request_response ~no_orders:true update_cs;
      Writer.write_cstruct w update_cs
    in
    let is_open o = match RespObj.string_exn o "ordStatus" with
    | "New" -> Some `Open
    | "PartiallyFilled" -> Some `Partially_filled
    | "PendingCancel" -> Some `Pending_cancel
    | _ -> None
    in
    let get_open_orders ?user_id ?order_id () = match user_id, order_id with
    | None, None ->
      Int.Table.fold order_table ~init:[] ~f:begin fun ~key:uid ~data:orders a ->
        Uuid.Table.fold orders ~init:a ~f:begin fun ~key:oid ~data:o a -> match is_open o with
        | Some status -> (status, o) :: a
        | None -> a
        end
      end
    | Some user_id, None ->
      begin match Int.Table.find order_table user_id with
      | None -> []
      | Some table ->
        Uuid.Table.fold table ~init:[] ~f:begin fun ~key:uid ~data:o a -> match is_open o with
        | Some status -> (status, o) :: a
        | None -> a
        end
      end
    | Some user_id, Some order_id ->
      begin match Int.Table.find order_table user_id with
      | None -> []
      | Some table -> begin match Uuid.Table.find table order_id with
        | None -> raise No_such_order
        | Some o -> match is_open o with None -> [] | Some status -> [status, o]
        end
      end
    | None, Some order_id ->
      begin match Order.find order_table order_id with
      | None -> raise No_such_order
      | Some (uid, o) -> (match is_open o with Some status -> [status, o] | None -> [])
      end
    in
    begin try
      let user_id = Option.(cut_trade_account trade_account >>| snd) in
      let order_id = try Option.some @@ Uuid.of_string order with _ -> None in
      match get_open_orders ?user_id ?order_id () with
      | [] ->
        send_no_open_orders ~request_id:id;
        Log.debug log_bitmex "[%s] -> OpenOrdersResp (%ld) 0 open orders" addr_str id
      | oos ->
        let nb_msgs = List.length oos in
        List.iteri oos ~f:(fun i -> send_order_update ~request_id:id ~nb_msgs ~msg_number:(succ i));
        Log.debug log_bitmex "[%s] -> OpenOrdersResp (%ld) %d open orders" addr_str id nb_msgs
      | exception No_such_order ->
        Open.Reject.write reject_cs ~request_id:id "No such order";
        Writer.write_cstruct w reject_cs;
        Log.error log_bitmex "[%s] -> OpenOrdersRej (%ld) No such order" addr_str id
    with (Invalid_argument _) ->
      Open.Reject.write reject_cs ~request_id:id "Invalid trade account syntax %s" trade_account;
      Writer.write_cstruct w reject_cs;
      Log.error log_bitmex "[%s] -> OpenOrdersRej (%ld) Invalid trade account syntax" addr_str id
    end

  | Some CurrentPositionsRequest ->
    let open Trading.Position in
    let { Request.id; trade_account } = Request.read msg_cs in
    let reject_cs = Cstruct.of_bigarray ~off:0 ~len:Reject.sizeof_cs scratchbuf in
    let update_cs = Cstruct.of_bigarray ~off:0 ~len:Update.sizeof_cs scratchbuf in
    let ({ Connection.addr_str; position } as c) = Option.value_exn conn in
    Log.debug log_dtc "<- [%s] PosReq (%s)" addr_str trade_account;
    let write_position ~nb_msgs ~msg_number ~request_id ~trade_account p =
      let userid = RespObj.int_exn p "account" in
      with_userid c ~userid ~f:begin fun ~addr_str ~userid ~username ~key:_ ~secret:_ ->
        Update.write
          ~nb_msgs
          ~msg_number
          ~request_id
          ~symbol:RespObj.(string_exn p "symbol")
          ~exchange:!my_exchange
          ~trade_account
          ~p:RespObj.(float_exn p "avgEntryPrice")
          ~v:RespObj.(int64_exn p "currentQty" |> Int64.to_float)
          update_cs;
        Writer.write_cstruct w update_cs
      end
    in
    let get_open_positions user_id =
      IS.Table.fold position ~init:(0, []) ~f:begin fun ~key:(user_id', symbol) ~data ((nb_open_ps, open_ps) as acc) ->
        match RespObj.bool data "isOpen" with
        | None ->
          Log.error log_bitmex "%s" @@ Yojson.Safe.to_string @@ RespObj.to_json data;
          acc
        | Some false -> acc
        | Some true -> match user_id with
        | Some user_id when user_id = user_id' -> succ nb_open_ps, data :: open_ps
        | Some _user_id -> acc
        | None -> succ nb_open_ps, data :: open_ps
      end
    in
    begin try
      let maybe_user_id = Option.(cut_trade_account trade_account >>| snd) in
      let nb_msgs, open_positions = get_open_positions maybe_user_id in
      List.iteri open_positions ~f:begin fun i p ->
        write_position ~nb_msgs ~msg_number:(succ i) ~request_id:id ~trade_account p
      end;
      if nb_msgs = 0 then begin
        Update.write ~nb_msgs:1 ~msg_number:1 ~request_id:id ~trade_account ~no_positions:true update_cs;
        Writer.write_cstruct w update_cs
      end;
      Log.debug log_dtc "-> [%s] %d positions" addr_str nb_msgs;
    with (Invalid_argument _) ->
      Reject.write reject_cs ~request_id:id "Invalid trade_account syntax %s" trade_account
    end

  | Some HistoricalOrderFillsRequest ->
    let open Trading.Order.Fills in
    let m = Request.read msg_cs in
    let reject_cs = Cstruct.of_bigarray scratchbuf ~len:Reject.sizeof_cs in
    let response_cs = Cstruct.of_bigarray scratchbuf ~len:Response.sizeof_cs in
    let ({ Connection.addr_str; apikeys } as c) = Option.value_exn conn in
    Log.debug log_dtc "<- [%s] HistFillsReq (%s)" addr_str m.trade_account;
    let uri = Uri.with_path !base_uri "/api/v1/execution/tradeHistory" in
    let req = List.filter_opt [
        if m.Request.srv_order_id <> "" then Some ("orderID", `String m.Request.srv_order_id) else None;
        Some ("execType", `String "Trade")
      ]
    in
    let uri = Uri.with_query' uri ["filter", Yojson.Safe.to_string @@ `Assoc req] in
    let process = function
    | [] ->
      Response.write ~nb_msgs:1 ~msg_number:1 ~request_id:m.Request.id ~no_order_fills:true response_cs;
      Writer.write_cstruct w response_cs
    | fills ->
      let nb_msgs = List.length fills in
      List.iteri fills ~f:begin fun i o ->
        let o = RespObj.of_json o in
        let userid = RespObj.int_exn o "account" in
        with_userid c ~userid ~f:begin fun ~addr_str:_ ~userid ~username ~key:_ ~secret:_ ->
          Response.write
            ~nb_msgs
            ~msg_number:(succ i)
            ~request_id:m.Request.id
            ~symbol:RespObj.(string_exn o "symbol")
            ~exchange:!my_exchange
            ~srv_order_id:RespObj.(string_exn o "orderID" |> b64_of_uuid)
            ~p:RespObj.(float_exn o "avgPx")
            ~v:Float.(of_int64 RespObj.(int64_exn o "orderQty"))
            ~ts:(RespObj.string_exn o "transactTime" |> Time_ns.of_string)
            ?side:(RespObj.string_exn o "side" |> side_of_bmex)
            ~exec_id:RespObj.(string_exn o "execID" |> b64_of_uuid)
            ~trade_account:(trade_accountf ~userid ~username)
            response_cs;
          Writer.write_cstruct w response_cs
        end
      end;
      Log.debug log_dtc "-> [%s] HistOrdFillsResp %d" addr_str nb_msgs
    in
    let get_all_fills ~key ~secret =
      Rest.call ~name:"execution" ~f:begin fun uri ->
        Client.get ~headers:(Rest.mk_headers ~key ~secret `GET uri) uri
      end uri >>| function
      | Ok (`List orders) -> orders
      | Ok json -> Log.error log_bitmex "%s" @@ Yojson.Safe.to_string json; []
      | Error err -> Log.error log_bitmex "%s" @@ Error.to_string_hum err; []
    in
    begin
      match Option.(cut_trade_account m.trade_account >>| snd) with
      | Some userid -> begin match Int.Table.find apikeys userid with
        | Some { key; secret } -> don't_wait_for (get_all_fills ~key ~secret >>| process)
        | None ->
          Reject.write reject_cs ~request_id:m.id "No such account %s" m.trade_account;
          Writer.write_cstruct w reject_cs
        end
      | None -> don't_wait_for begin
          Deferred.List.fold ~init:[] (Int.Table.to_alist apikeys) ~f:begin fun a (userid, { key; secret }) ->
            get_all_fills ~key ~secret >>| List.rev_append a
          end >>| process
        end
      | exception (Invalid_argument _) ->
        Reject.write reject_cs ~request_id:m.id "Invalid trade account syntax %s" m.trade_account;
        Writer.write_cstruct w reject_cs
    end

  | Some TradeAccountsRequest ->
    let open Account.List in
    let { Request.id } = Request.read msg_cs in
    let c = Option.value_exn conn in
    Log.debug log_dtc "<- [%s] TradeAccountsReq (%ld)" c.addr_str id;
    c.ta_request_id <- id;
    write_trade_accounts c w

  | Some AccountBalanceRequest ->
    let open Account.Balance in
    let { Request.id; trade_account } = Request.read msg_cs in
    let { Connection.addr_str; margin; usernames } = Option.value_exn conn in
    let r_cs = Cstruct.of_bigarray ~off:0 ~len:Reject.sizeof_cs scratchbuf in
    let update_cs = Cstruct.of_bigarray ~off:0 ~len:Update.sizeof_cs scratchbuf in
    let reject k = Printf.ksprintf begin fun reason ->
        Reject.write r_cs ~request_id:id "%s" reason;
        Writer.write_cstruct w r_cs;
        Log.debug log_dtc "-> [%s] AccountBalanceRej" addr_str;
      end k
    in
    let write_no_balances request_id =
      Account.Balance.Update.write ~request_id ~nb_msgs:1 ~msg_number:1 ~no_account_balance:true update_cs;
      Writer.write_cstruct w update_cs;
      Log.debug log_dtc "-> [%s] no account balance" addr_str
    in
    let update ~msg_number ~nb_msgs ~username ~userid obj =
      write_balance_update ~unsolicited:false ~msg_number ~nb_msgs ~username ~userid update_cs obj;
      Writer.write_cstruct w update_cs;
      Log.debug log_dtc "-> [%s] account balance: %s:%d" addr_str username userid
    in
    Log.debug log_dtc "<- [%s] AccountBalanceReq (%s)" addr_str trade_account;
    let nb_msgs = IS.Table.length margin in
    if nb_msgs = 0 then
      write_no_balances id
    else if trade_account = "" then
      IS.Table.fold margin ~init:1 ~f:begin fun ~key:(userid, currency) ~data msg_number ->
        Int.Table.find_and_call usernames userid
          ~if_not_found:ignore
          ~if_found:begin fun usernames ->
            List.iter usernames ~f:(fun username -> update ~msg_number ~nb_msgs ~username ~userid data)
          end;
        succ msg_number
      end |> ignore
    else begin
      match Option.(cut_trade_account trade_account >>= fun (username, userid) ->
                    IS.Table.find margin (userid, "XBt") >>| fun obj -> username, userid, obj)
      with
      | Some (username, userid, obj) -> update ~msg_number:1 ~nb_msgs:1 ~username ~userid obj
      | None -> write_no_balances id
      | exception _ -> reject "Unknown trade account: %s" trade_account
    end

  | Some SubmitNewSingleOrder ->
    let module S = Trading.Order.Submit in
    let module U = Trading.Order.Update in
    let m = S.read msg_cs in
    let { Connection.addr_str; apikeys } as c = Option.value_exn conn in
    let order_update_cs = Cstruct.of_bigarray ~off:0 ~len:U.sizeof_cs scratchbuf in
    Log.debug log_dtc "<- [%s] %s" addr_str (S.show m);
    let accept_exn username userid =
      let uri = Uri.with_path !base_uri "/api/v1/order" in
      let qty = match Option.value_exn ~message:"side is undefined" m.S.side with
      | `Buy -> m.S.qty
      | `Sell -> Float.neg m.S.qty
      in
      let tif = Option.value_exn ~message:"tif is undefined" m.tif in
      let ordType = Option.value_exn ~message:"ordType is undefined" m.ord_type in
      let tif = match tif with
      | `Good_till_date_time -> invalid_arg "good_till_date_time"
      | #time_in_force as tif -> tif in
      let body =
        ["symbol", `String m.S.symbol;
         "orderQty", `Float qty;
         "timeInForce", `String (string_of_tif tif);
         "ordType", `String (string_of_ord_type ordType);
         "clOrdID", `String m.S.cli_ord_id;
         "text", `String m.text;
        ]
        @ price_fields_of_dtc ordType ~p1:m.S.p1 ~p2:m.S.p2
        @ execInst_of_dtc ordType tif `LastPrice
      in
      let body_str = Yojson.Safe.to_string @@ `Assoc body in
      let body = Body.of_string body_str in
      Log.debug log_bitmex "-> %s" body_str;
      Int.Table.find_and_call apikeys userid
        ~if_not_found:begin fun _ ->
          Dtc_util.Trading.Order.Submit.reject order_update_cs m "No API key for %s:%d" username userid;
          Writer.write_cstruct w order_update_cs;
          Log.error log_bitmex "No API key for %s:%d" username userid;
          Deferred.unit
        end
        ~if_found:begin fun { key; secret } ->
          Rest.call ~extract_exn:true ~name:"submit" ~f:begin fun uri ->
            Client.post ~chunked:false ~body
              ~headers:(Rest.mk_headers ~key ~secret ~data:body_str `POST uri) uri
          end uri >>| function
          | Ok _body -> ()
          | Error err ->
            let msg = match Error.to_exn err with Failure msg -> msg | exn -> Exn.to_string_mach exn in
            Dtc_util.Trading.Order.Submit.reject order_update_cs m "%s" msg;
            Writer.write_cstruct w order_update_cs;
            Log.error log_bitmex "%s" msg
        end
    in
    if !my_exchange <> m.S.exchange then begin
      Dtc_util.Trading.Order.Submit.reject order_update_cs m "Unknown exchange";
      Writer.write_cstruct w order_update_cs;
    end
    else if m.S.tif = Some `Good_till_date_time then begin
      Dtc_util.Trading.Order.Submit.reject order_update_cs m "BitMEX does not support TIF Good till datetime";
      Writer.write_cstruct w order_update_cs;
    end
    else
    let on_exn _ =
      Dtc_util.Trading.Order.Submit.update
        ~status:`Rejected ~reason:New_order_rejected
        order_update_cs m
        "exception raised when trying to submit %s" m.S.cli_ord_id;
      Writer.write_cstruct w order_update_cs;
      Deferred.unit
    in
    don't_wait_for @@ eat_exn ~on_exn begin fun () ->
      let username, userid = Option.value_exn (cut_trade_account m.trade_account) in
      accept_exn username userid
    end

  | Some CancelReplaceOrder ->
    let open Trading.Order in
    let reject ~userid ~username m k =
      let trade_account = trade_accountf ~userid ~username in
      let order_update_cs = Cstruct.of_bigarray ~off:0
          ~len:Update.sizeof_cs scratchbuf
      in
      Printf.ksprintf begin fun reason ->
        Update.write
          ~nb_msgs:1
          ~msg_number:1
          ~reason:Cancel_replace_rejected
          ~cli_ord_id:m.Replace.cli_ord_id
          ~srv_ord_id:m.srv_ord_id
          ?ord_type:m.Replace.ord_type
          ~p1:m.Replace.p1
          ~p2:m.Replace.p2
          ~order_qty:m.Replace.qty
          ?tif:m.Replace.tif
          ~good_till_ts:m.Replace.good_till_ts
          ~trade_account
          ~info_text:reason
          order_update_cs;
        Writer.write_cstruct w order_update_cs;
        Log.debug log_dtc "-> [%s] CancelReplaceRej: %s" addr_str reason
      end k
    in
    let m = Replace.read msg_cs in
    let ({ Connection.addr_str; order } as c) = Option.value_exn conn in
    Log.debug log_dtc "<- [%s] %s" addr_str (Replace.show m);
    let uri = Uri.with_path !base_uri "/api/v1/order" in
    let update_order ~userid ~username ~key ~secret ~body ~body_str =
      Rest.call ~extract_exn:true ~name:"amend" ~f:begin fun uri ->
        Client.put
          ~chunked:false ~body
          ~headers:(Rest.mk_headers ~key ~secret ~data:body_str `PUT uri)
          uri
      end uri >>| function
      | Ok body ->
        Log.debug log_bitmex "<- %s" @@ Yojson.Safe.to_string body
      | Error err ->
        let msg =  match Error.to_exn err with Failure msg -> msg | exn -> Exn.to_string_mach exn in
        Log.error log_bitmex "%s" msg;
        reject ~userid ~username m "%s" msg
    in
    let replace_order_exn order =
      let userid = RespObj.int_exn order "account" in
      let orderID = RespObj.string_exn order "orderID" in
      let ord_type = RespObj.string_exn order "ordType" in
      let ord_type = ord_type_of_string ord_type in
      let p1 = if m.p1_set then Some m.p1 else None in
      let p2 = if m.p2_set then Some m.p2 else None in
      let body = List.filter_opt [
          Some ("orderID", `String orderID);
          if m.Replace.qty <> 0. then Some ("orderQty", `Float m.Replace.qty) else None;
        ] @ begin match ord_type with
        | `Stop_limit when RespObj.bool_exn order "workingIndicator" ->
          Option.value_map p2 ~default:[] ~f:(fun p2 -> ["price", `Float p2])
        | #order_type -> price_fields_of_dtc ord_type ?p1 ?p2
        end
      in
      let body_str = Yojson.Safe.to_string @@ `Assoc body in
      let body = Body.of_string body_str in
      Log.debug log_bitmex "-> %s" body_str;
      with_userid c ~userid ~f:begin fun ~addr_str:_ ~userid ~username ~key ~secret ->
        don't_wait_for @@ update_order ~userid ~username ~key ~secret ~body ~body_str
      end
    in
    begin match Order.find order (Uuid.of_string @@ uuid_of_b64 m.srv_ord_id) with
    | None ->
      Log.error log_bitmex "CancelReplace: cannot find order, SC will not be notified"
    | Some (userid, o) ->
      if Option.is_some m.Replace.ord_type then
        with_userid c ~userid ~f:(fun ~addr_str:_ ~userid ~username ~key:_ ~secret:_ ->
            reject ~userid ~username m "Modification of order type is not supported by BitMEX")
      else if Option.is_some m.Replace.tif then
        with_userid c ~userid ~f:(fun ~addr_str:_ ~userid ~username ~key:_ ~secret:_ ->
            reject ~userid ~username m "Modification of time in force is not supported by BitMEX")
      else
      try replace_order_exn o with _ ->
        with_userid c ~userid ~f:(fun ~addr_str:_ ~userid ~username ~key:_ ~secret:_ ->
            reject ~userid ~username m "internal error when canceling %s" m.srv_ord_id)
    end

  | Some CancelOrder ->
    let open Trading.Order in
    let ({ Connection.addr_str; order } as c) = Option.value_exn conn in
    let { Cancel.srv_ord_id; cli_ord_id } = Cancel.read msg_cs in
    let reject ~userid ~username k =
      let trade_account = trade_accountf ~userid ~username in
      let order_update_cs = Cstruct.of_bigarray ~off:0 ~len:Update.sizeof_cs scratchbuf in
      Printf.ksprintf begin fun reason ->
        Update.write
          ~nb_msgs:1
          ~msg_number:1
          ~reason:Cancel_rejected
          ~cli_ord_id
          ~srv_ord_id
          ~trade_account
          ~info_text:reason
          order_update_cs;
        Writer.write_cstruct w order_update_cs;
        Log.debug log_dtc "-> [%s] CancelOrderRej: %s" addr_str reason
      end k
    in
    let hex_srv_ord_id = uuid_of_b64 srv_ord_id in
    let { Connection.addr_str } = Option.value_exn conn in
    Log.debug log_dtc "<- [%s] Cancelorder cli=%s srv=%s" addr_str cli_ord_id hex_srv_ord_id;

    let cancel_order ~userid ~username ~key ~secret hex_srv_ord_id =
      let uri = Uri.with_path !base_uri "/api/v1/order" in
      let body_str = `Assoc ["orderID", `String hex_srv_ord_id] |> Yojson.Safe.to_string in
      let body = Body.of_string body_str in
      Rest.call ~extract_exn:true ~name:"cancel" ~f:begin fun uri ->
        Client.delete
          ~chunked:false ~body
          ~headers:(Rest.mk_headers ~key ~secret ~data:body_str `DELETE uri) uri
      end uri >>| function
      | Ok body ->
        Log.debug log_bitmex "<- %s" @@ Yojson.Safe.to_string body
      | Error err ->
        let msg = match Error.to_exn err with Failure msg -> msg | exn -> Exn.to_string_mach exn in
        reject ~userid ~username "%s" msg;
        Log.error log_bitmex "%s" msg
    in
    begin match Order.find order (Uuid.of_string hex_srv_ord_id) with
    | None ->
      Log.error log_bitmex "Cancel: cannot find order, SC will not be notified"
    | Some (userid, o) ->
      with_userid c ~userid ~f:begin fun ~addr_str:_ ~userid ~username ~key ~secret ->
        don't_wait_for @@ cancel_order ~userid ~username ~key ~secret hex_srv_ord_id
      end
    end

  | Some _
  | None ->
    let buf = Buffer.create 128 in
    Cstruct.hexdump_to_buffer buf msg_cs;
    Log.error log_dtc "%s" @@ Buffer.contents buf

let dtcserver ~server ~port =
  let server_fun addr r w =
    don't_wait_for begin
      Condition.wait ws_feed_connected >>= fun () ->
      Deferred.all_unit [Writer.close w ; Reader.close r]
    end ;
    let addr_str = InetAddr.to_string addr in
    (* So that process does not allocate all the time. *)
    let rec handle_chunk w consumed buf ~pos ~len =
      if len < 2 then return @@ `Consumed (consumed, `Need_unknown)
      else
      let msglen = Bigstring.unsafe_get_int16_le buf ~pos in
      if len < msglen then return @@ `Consumed (consumed, `Need msglen)
      else begin
        let msg_cs = Cstruct.of_bigarray buf ~off:pos ~len:msglen in
        process addr w msg_cs scratchbuf;
        handle_chunk w (consumed + msglen) buf (pos + msglen) (len - msglen)
      end
    in
    let cleanup () =
      Log.info log_dtc "client %s disconnected" Socket.Address.(to_string addr);
      (match String.Table.find Connection.active addr_str with
      | None -> Deferred.unit
      | Some c ->
        Connection.purge c;
        Pipe.write client_deleted_w c
      ) >>| fun () ->
      String.Table.remove Connection.active addr_str
    in
    Monitor.protect ~name:"server_fun" ~finally:cleanup (fun () ->
        Reader.(read_one_chunk_at_a_time r ~handle_chunk:(handle_chunk w 0))
      ) |> Deferred.ignore
  in
  let on_handler_error_f addr exn =
    Log.error log_dtc "on_handler_error (%s): %s"
      Socket.Address.(to_string addr) Exn.(to_string exn)
  in
  Conduit_async.serve
    ~on_handler_error:(`Call on_handler_error_f)
    server (Tcp.on_port port) server_fun

let send_instr_update_msgs w buf_cs instr symbol_id =
  let open RespObj in
  Option.iter (int64 instr "volume")
    ~f:(fun v ->
        let open MarketData.UpdateSession in
        let buf_cs = Cstruct.of_bigarray buf_cs ~len:sizeof_cs in
        write ~kind:`Volume ~symbol_id ~data:Int64.(to_float v) buf_cs;
        Writer.write_cstruct w buf_cs
      );
  Option.iter (float instr "lowPrice")
    ~f:(fun p ->
        let open MarketData.UpdateSession in
        let buf_cs = Cstruct.of_bigarray buf_cs ~len:sizeof_cs in
        write ~kind:`Low ~symbol_id ~data:p buf_cs;
        Writer.write_cstruct w buf_cs
      );
  Option.iter (float instr "highPrice")
    ~f:(fun p ->
        let open MarketData.UpdateSession in
        let buf_cs = Cstruct.of_bigarray buf_cs ~len:sizeof_cs in
        write ~kind:`High ~symbol_id ~data:p buf_cs;
        Writer.write_cstruct w buf_cs
      );
  Option.iter (int64 instr "openInterest")
    ~f:(fun p ->
        let open MarketData.UpdateOpenInterest in
        let buf_cs = Cstruct.of_bigarray buf_cs ~len:sizeof_cs in
        write ~symbol_id ~open_interest:Int64.(to_int32_exn p) buf_cs;
        Writer.write_cstruct w buf_cs
      );
  Option.iter (float instr "prevClosePrice")
    ~f:(fun p ->
        let open MarketData.UpdateSession in
        let buf_cs = Cstruct.of_bigarray buf_cs ~len:sizeof_cs in
        write ~kind:`Open ~symbol_id ~data:p buf_cs;
        Writer.write_cstruct w buf_cs
      )

let delete_instr instrObj =
  let instrObj = RespObj.of_json instrObj in
  Log.info log_bitmex "deleted instrument %s" RespObj.(string_exn instrObj "symbol")

let insert_instr buf_cs instrObj =
  let buf_cs = Cstruct.of_bigarray buf_cs ~len:SecurityDefinition.Response.sizeof_cs in
  let instrObj = RespObj.of_json instrObj in
  let key = RespObj.string_exn instrObj "symbol" in
  let secdef = Instrument.RespObj.to_secdef ~testnet:!use_testnet instrObj in
  let instr = Instrument.create ~instrObj ~secdef () in
  let books = Int.Table.{ bids = create () ; asks = create () ; } in
  String.Table.set Instrument.active key instr;
  String.Table.set orderbooks ~key ~data:books;
  Log.info log_bitmex "inserted instrument %s" key;
  (* Send secdef response to clients. *)
  let on_connection { Connection.addr; addr_str; w; subs; send_secdefs } =
    if send_secdefs then begin
      SecurityDefinition.Response.to_cstruct buf_cs { secdef with final = true; request_id = 110_000_000l };
      Writer.write_cstruct w buf_cs
    end
  in
  String.Table.iter Connection.active ~f:on_connection

let update_instr buf_cs instrObj =
  let instrObj = RespObj.of_json instrObj in
  let symbol = RespObj.string_exn instrObj "symbol" in
  match String.Table.find Instrument.active symbol with
  | None ->
    Log.error log_bitmex "update_instr: unable to find %s" symbol;
  | Some instr ->
    instr.instrObj <- RespObj.merge instr.instrObj instrObj;
    Log.debug log_bitmex "updated instrument %s" symbol;
    (* Send messages to subscribed clients according to the type of update. *)
    let on_connection { Connection.addr; addr_str; w; subs; _ } =
      let on_symbol_id symbol_id =
        send_instr_update_msgs w buf_cs instrObj symbol_id;
        Log.debug log_dtc "-> [%s] instrument %s" addr_str symbol
      in
      Option.iter String.Table.(find subs symbol) ~f:on_symbol_id
    in
    String.Table.iter Connection.active ~f:on_connection

let update_depths update_cs action { OrderBook.L2.symbol; id; side; size; price } =
  (* find_exn cannot raise here *)
  let { bids; asks } =  String.Table.find_exn orderbooks symbol in
  let side = side_of_bmex side in
  let table = match side with
  | Some `Buy -> bids
  | Some `Sell -> asks
  | None -> failwith "update_depth: empty side" in
  let price = match price with
  | Some p -> Some p
  | None -> begin match Int.Table.find table id with
    | Some { price } -> Some price
    | None -> None
    end
  in
  let size = match size with
  | Some s -> Some s
  | None -> begin match Int.Table.find table id with
    | Some { size } -> Some size
    | None -> None
    end
  in
  match price, size with
  | Some price, Some size ->
    begin match action with
    | OB.Partial | Insert | Update -> Int.Table.set table id { size ; price }
    | Delete -> Int.Table.remove table id
    end;
    let on_connection { Connection.addr; addr_str; w; subs; subs_depth; _} =
      let on_symbol_id symbol_id =
        (* Log.debug log_dtc "-> [%s] depth %s %s %s %f %d" addr_str (OB.sexp_of_action action |> Sexp.to_string) symbol side price size; *)
        MarketDepth.Update.write
          ~op:(match action with Partial | Insert | Update -> `Insert_update | Delete -> `Delete)
          ~p:price
          ~v:(Float.of_int size)
          ~symbol_id
          ?side
          update_cs;
        Writer.write_cstruct w update_cs
      in
      Option.iter String.Table.(find subs_depth symbol) ~f:on_symbol_id
    in
    String.Table.iter Connection.active ~f:on_connection
  | _ ->
    Log.info log_bitmex "update_depth: received update before snapshot, ignoring"

let update_quote update_cs q =
  let old_q = String.Table.find_or_add quotes q.Quote.symbol ~default:(fun () -> q) in
  let merged_q = Quote.merge old_q q in
  let bidPrice = Option.value ~default:Float.max_finite_value merged_q.bidPrice in
  let bidSize = Option.value ~default:0 merged_q.bidSize in
  let askPrice = Option.value ~default:Float.max_finite_value merged_q.askPrice in
  let askSize = Option.value ~default:0 merged_q.askSize in
  String.Table.set quotes ~key:q.symbol ~data:merged_q;
  Log.debug log_bitmex "set quote %s" q.symbol;
  let on_connection { Connection.addr; addr_str; w; subs; subs_depth; _} =
    let on_symbol_id symbol_id =
      Log.debug log_dtc "-> [%s] bidask %s %f %d %f %d"
        addr_str q.symbol bidPrice bidSize askPrice askSize;
      MarketData.UpdateBidAsk.write
        ~symbol_id
        ~bid:bidPrice
        ~bid_qty:Float.(of_int bidSize)
        ~ask:askPrice
        ~ask_qty:Float.(of_int askSize)
        ~ts:(Time_ns.of_string merged_q.timestamp)
        update_cs;
      Writer.write_cstruct w update_cs
    in
    match String.Table.(find subs q.symbol, find subs_depth q.symbol) with
    | Some id, None -> on_symbol_id id
    | _ -> ()
  in
  String.Table.iter Connection.active ~f:on_connection

let update_trade cs { Trade.symbol; timestamp; price; size; side; tickDirection; trdMatchID;
                      grossValue; homeNotional; foreignNotional; id } =
  let open Trade in
  Log.debug log_bitmex "trade %s %s %f %d" symbol side price size;
  match side_of_bmex side, String.Table.find Instrument.active symbol with
  | None, _ -> ()
  | _, None ->
    Log.error log_bitmex "update_trade: found no instrument for %s" symbol
  | Some s, Some instr ->
    instr.last_trade_price <- price;
    instr.last_trade_size <- size;
    instr.last_trade_ts <- Time_ns.of_string timestamp;
    (* Send trade updates to subscribers. *)
    let on_connection { Connection.addr; addr_str; w; subs; _} =
      let on_symbol_id symbol_id =
        MarketData.UpdateTrade.write
          ~symbol_id
          ~side:s
          ~p:price
          ~v:(Int.to_float size)
          ~ts:(Time_ns.of_string timestamp)
          cs;
        Writer.write_cstruct w cs;
        Log.debug log_dtc "-> [%s] trade %s %s %f %d" addr_str symbol side price size
      in
      Option.iter String.Table.(find subs symbol) ~f:on_symbol_id
    in
    String.Table.iter Connection.active ~f:on_connection

type bitmex_th = {
  th: unit Deferred.t ;
  ws: Yojson.Safe.json Pipe.Reader.t ;
}

let close_bitmex_ws { ws } = Pipe.close_read ws

let bitmex_ws
    ~instrs_initialized ~orderbook_initialized ~quotes_initialized =
  let open Ws in
  let buf_cs = Bigstring.create 4096 in
  let trade_update_cs = Cstruct.of_bigarray ~len:MarketData.UpdateTrade.sizeof_cs buf_cs in
  let depth_update_cs = Cstruct.of_bigarray ~len:MarketDepth.Update.sizeof_cs buf_cs in
  let bidask_update_cs = Cstruct.of_bigarray ~len:MarketData.UpdateBidAsk.sizeof_cs buf_cs in
  let on_update update =
    let action = update_action_of_string update.action in
    match action, update.table, update.data with
    | Update, "instrument", instrs ->
      if Ivar.is_full instrs_initialized then List.iter instrs ~f:(update_instr buf_cs)
    | Delete, "instrument", instrs ->
      if Ivar.is_full instrs_initialized then List.iter instrs ~f:delete_instr
    | _, "instrument", instrs ->
      List.iter instrs ~f:(insert_instr buf_cs);
      Ivar.fill_if_empty instrs_initialized ()
    | _, "orderBookL2", depths ->
      let filter_f json = match OrderBook.L2.of_yojson json with
      | Ok u -> Some u
      | Error reason ->
        Log.error log_bitmex "%s: %s (%s)"
          reason Yojson.Safe.(to_string json) update.action;
        None
      in
      let depths = List.filter_map depths ~f:filter_f in
      let depths = List.group depths
          ~break:(fun { symbol } { symbol=symbol' } -> symbol <> symbol')
      in
      don't_wait_for begin
        Ivar.read instrs_initialized >>| fun () ->
        List.iter depths ~f:begin function
        | [] -> ()
        | h::t as ds ->
          Log.debug log_bitmex "depth update %s" h.symbol;
          List.iter ds ~f:(update_depths depth_update_cs action)
        end;
        Ivar.fill_if_empty orderbook_initialized ()
      end
    | _, "trade", trades ->
      let open Trade in
      let iter_f t = match Trade.of_yojson t with
      | Error msg ->
        Log.error log_bitmex "%s" msg
      | Ok t -> update_trade trade_update_cs t
      in
      don't_wait_for begin
        Ivar.read instrs_initialized >>| fun () ->
        List.iter trades ~f:iter_f
      end
    | _, "quote", quotes ->
      let filter_f json =
        match Quote.of_yojson json with
        | Ok q -> Some q
        | Error reason ->
          Log.error log_bitmex "%s: %s (%s)"
            reason Yojson.Safe.(to_string json) update.action;
          None
      in
      let quotes = List.filter_map quotes ~f:filter_f in
      List.iter quotes ~f:(update_quote bidask_update_cs);
      Ivar.fill_if_empty quotes_initialized ()
    | _, table, json ->
      Log.error log_bitmex "Unknown/ignored BitMEX DB table %s or wrong json %s"
        table Yojson.Safe.(to_string @@ `List json)
  in
  let to_ws, to_ws_w = Pipe.create () in
  let bitmex_topics = ["instrument"; "quote"; "orderBookL2"; "trade"] in
  let clients_topics = ["order"; "execution"; "position"; "margin"] in
  let subscribe_topics ?(topic="") ~id ~topics =
    let payload =
      create_request
        ~op:"subscribe"
        ~args:(`List (List.map topics ~f:(fun t -> `String t)))
        ()
    in
    MD.message id topic (request_to_yojson payload)
  in
  let my_uuid = Uuid.(to_string @@ create ()) in
  let make_stream_id ~addr ~id = my_uuid ^ "|" ^ addr ^ "|" ^ Int.to_string id in
  let addr_id_of_stream_id_exn stream_id = match String.split ~on:'|' stream_id with
  | [_; client_addr; userid] -> client_addr, Int.of_string userid
  | _ -> invalid_arg "addr_id_of_stream_id_exn"
  in
  let subscribe_client ?(topic="") ~addr ~id () =
    let id = make_stream_id ~addr ~id in
    Pipe.write to_ws_w @@ MD.to_yojson @@ MD.subscribe ~id ~topic
  in
  let unsubscribe_client ?(topic="") ~addr ~id () =
    let id = make_stream_id ~addr ~id in
    Pipe.write to_ws_w @@ MD.to_yojson @@ MD.unsubscribe ~id ~topic
  in
  let connected = Mvar.create () in
  let rec resubscribe () =
    Mvar.take (Mvar.read_only connected) >>= fun () ->
    Condition.broadcast ws_feed_connected () ;
    String.Table.iter Connection.active ~f:(fun c -> c.need_resubscribe <- true);
    Pipe.write to_ws_w @@ MD.to_yojson @@ MD.subscribe ~id:my_uuid ~topic:"" >>= fun () ->
    resubscribe ()
  in
  don't_wait_for @@ Pipe.iter_without_pushback ~continue_on_error:true new_client_accepted ~f:begin fun { to_bitmex_r } ->
    don't_wait_for @@ Pipe.iter ~continue_on_error:true to_bitmex_r ~f:begin function
    | Subscribe { id; addr } -> subscribe_client ~addr ~id ()
    | Unsubscribe { id; addr } -> unsubscribe_client ~addr ~id ()
    end
  end;
  don't_wait_for @@ Pipe.iter client_deleted ~f:begin fun { addr_str=addr; usernames } ->
    let usernames = Int.Table.to_alist usernames in
    Deferred.List.iter usernames ~how:`Parallel ~f:(fun (id, _) -> unsubscribe_client ~addr ~id ())
  end;
  don't_wait_for @@ resubscribe ();
  let ws = open_connection ~connected ~to_ws ~log:log_ws
      ~testnet:!use_testnet ~md:true ~topics:[] () in
  let on_ws_msg msg = match MD.of_yojson msg with
  | Error msg ->
    Log.error log_bitmex "%s" msg;
    Deferred.unit
  | Ok { MD.typ = Unsubscribe; id = stream_id; payload = None } ->
    let client_addr, user_id = addr_id_of_stream_id_exn stream_id in
    begin match String.Table.find Connection.active client_addr with
    | None ->
      Log.info log_bitmex "BitMEX unsubscribed stream for client %s, but client not in table" client_addr;
      Deferred.unit
    | Some c ->
      Pipe.write c.to_client_w @@ Unsubscribe { addr=client_addr; id=user_id }
    end
  | Ok { MD.typ; id = stream_id; payload = Some payload } -> begin
      match addr_id_of_stream_id_exn stream_id with
      | exception (Invalid_argument _) -> begin match Ws.msg_of_yojson payload with
        (* Server *)
        | Welcome ->
          Pipe.write to_ws_w @@ MD.to_yojson @@ subscribe_topics my_uuid bitmex_topics
        | Error msg ->
          Log.error log_bitmex "BitMEX: error %s" @@ show_error msg;
          Deferred.unit
        | Ok { request = { op = "subscribe" }; subscribe; success } ->
          Log.info log_bitmex "BitMEX: subscribed to %s: %b" subscribe success;
          Deferred.unit
        | Ok { success } ->
          Log.error log_bitmex "BitMEX: unexpected response %s" (Yojson.Safe.to_string payload);
          Deferred.unit
        | Update update ->
          on_update update;
          Deferred.unit
        end
      | client_addr, userid -> begin match String.Table.find Connection.active client_addr with
        | None ->
          Log.info log_bitmex "Got %s for %s, but client not in table"
            (Yojson.Safe.to_string payload) client_addr;
          Deferred.unit
        | Some c -> match Ws.msg_of_yojson payload with
        (* Clients *)
        | Welcome ->
          with_userid c ~userid ~f:begin fun ~addr_str:_ ~userid ~username ~key ~secret ->
            Pipe.write_without_pushback to_ws_w @@ MD.to_yojson @@ MD.auth ~id:stream_id ~topic:"" ~key ~secret
          end;
          Deferred.unit
        | Error msg ->
          Log.error log_bitmex "[%s] %d: error %s" c.addr_str userid (show_error msg);
          Deferred.unit
        | Ok { request = { op = "authKey"}; success} ->
          Log.debug log_bitmex "[%s] %d: subscribe to topics" c.addr_str userid;
          Deferred.all_unit [
            Pipe.write to_ws_w @@ MD.to_yojson @@ subscribe_topics stream_id clients_topics;
            Pipe.write c.to_client_w @@ Subscribe { addr=client_addr; id=userid }
          ]
        | Ok { request = { op = "subscribe"}; subscribe; success} ->
          Log.info log_bitmex "[%s] %d: subscribed to %s: %b" c.addr_str userid subscribe success;
          Deferred.unit
        | Ok { success } ->
          Log.error log_bitmex "[%s] %d: unexpected response %s"
            c.addr_str userid (Yojson.Safe.to_string payload);
          Deferred.unit
        | Update update ->
          let client_update = { Connection.userid ; update } in
          Pipe.write c.ws_w client_update
        end
    end
  | _ ->
    Log.error log_bitmex "BitMEX: unexpected multiplexed packet format";
    Deferred.unit
  in
  let th =
    Monitor.handle_errors
      (fun () -> Pipe.iter ~continue_on_error:true ws ~f:on_ws_msg)
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
    Gc.full_major () ;
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
