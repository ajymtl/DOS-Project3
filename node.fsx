#r "nuget: Akka.FSharp"
open Akka.FSharp
open Akka.Actor
open System
open System.Collections.Generic
open System.Threading

let system = System.create "System" (Configuration.defaultConfig())
let args = System.Environment.GetCommandLineArgs()
let mutable boss: IActorRef = null
let baseNumber = 4

type StartBoss = {
    NumWorkers: int;
    NumRequests: int;
}

type StartWorker = {
    Id: int;
    MaxRows: int;
}

type AddFirstWorker = {
    WorkerGroup: List<int>;
}

type FinishRoute = {
    NumberOfHops: int;
}

type Job = {
    RequestFrom: int;
    RequestTo: int;
    HopCount: int;
}

type RowInfo = {
    RowIndex: int;
    RowData: List<int>;
}

type NeighborInfo = {
    WorkerIdList: List<int>;
}

type Acknowledgementnowledgement = {
    NewWorkerId: int;
}

type RoutingInfo = {
    WorkerCount: int;
    RequestCount: int;
}

type MessageType = 
    | StartBoss of StartBoss
    | JoinFinish
    | JoinWorkersInDT
    | StartRouting of RoutingInfo
    | StartRoutingBoss
    | FinishRoute of FinishRoute
    | WorkerNotFound
    | RouteToWorkerNotFound
    | StartWorker of StartWorker
    | AddFirstWorker of AddFirstWorker
    | JoinJob of Job
    | RouteJob of Job
    | UpdateRow of RowInfo
    | UpdateNeighborSet of NeighborInfo
    | SendAcknowledgementToBoss of Acknowledgementnowledgement
    | Acknowledgement

let integerToDigits baseN value : int list =
    let rec loop num digits =
        let q = num / baseN
        let r = num % baseN
        if q = 0 then 
            r :: digits 
        else 
            loop q (r :: digits)

    loop value []

let toBase4String(raw) =
    raw
    |> integerToDigits 4
    |> List.fold (fun acc x -> acc + x.ToString("X")) ""
    |> string

let toBase4StringTruncated(raw, length, baseNumber) =
    let mutable string = toBase4String raw
    let diff = length - string.Length
    if diff > 0 then
        let mutable j = 0
        while (j<diff) do
            string <- "0" + string
            j <- j+1
    string

let Worker (mailbox: Actor<_>) = 
    let mutable id: int = 0
    let mutable maxRows: int = 0
    let SmallerLeafs = new List<int>()
    let LargerLeafs = new List<int>()
    let mutable routingTable = Array2D.zeroCreate<int> 1 1
    let mutable backNumber: int = 0
    let mutable idSpace: int = 0

    let initRoutingTable() = 
        let initRow (row: int) = 
            [0 .. (baseNumber - 1)]
            |> List.iter (fun col -> routingTable.[row, col] <- -1)
            |> ignore
        
        routingTable <- Array2D.zeroCreate<int> maxRows baseNumber

        [0 .. (maxRows - 1)]
        |> List.iter(fun row -> initRow (row))
        |> ignore

    let updateLeafSet(nodes: List<int>) = 
        for i in nodes do
            if (i > id && not (LargerLeafs.Contains(i))) then
                if (LargerLeafs.Count < baseNumber) then
                    LargerLeafs.Add(i)
                else
                    let max = LargerLeafs |> Seq.max
                    if (i < max) then
                        LargerLeafs.Remove(max) |> ignore
                        LargerLeafs.Add(i)
            elif (i < id && not (SmallerLeafs.Contains(i))) then
                if (SmallerLeafs.Count < baseNumber) then
                    SmallerLeafs.Add(i)
                else 
                    let min = SmallerLeafs |> Seq.min
                    if (i > min) then
                        SmallerLeafs.Remove(min) |> ignore
                        SmallerLeafs.Add(i)
            else ()

    let rec checkPrefix(s1, s2) =
        if s1 <> "" && s2 <> "" && s1.[0] = s2.[0] then
            1 + checkPrefix (s1.[1..], s2.[1..])
        else
            0

    let updateSamePrefixTableEntries(idInBaseVal: string, nodes: List<int>) = 
        for node in nodes do
            let iInBaseVal = toBase4StringTruncated(node, maxRows, baseNumber)
            let index = checkPrefix(idInBaseVal, iInBaseVal)
            if (routingTable.[index, (iInBaseVal.[index] |> string |> int)] = -1) then
                routingTable.[index, (iInBaseVal.[index] |> string |> int)] <- node

    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with
            | StartWorker initMessage -> 
                id <- initMessage.Id
                maxRows <- initMessage.MaxRows
                idSpace <- Math.Pow (float baseNumber, float maxRows) |> int

                initRoutingTable()
            | AddFirstWorker addFirstWorkerMessage -> 
                addFirstWorkerMessage.WorkerGroup.Remove(id) |> ignore

                updateLeafSet(addFirstWorkerMessage.WorkerGroup)
                
                let idInBaseVal = toBase4StringTruncated(id, maxRows, baseNumber)
                
                updateSamePrefixTableEntries(idInBaseVal, addFirstWorkerMessage.WorkerGroup)

                let col(row: int) = idInBaseVal.[row] |> string |> int
                [0 .. (maxRows - 1)]
                |> List.iter(fun row -> routingTable.[row, col(row)] <- id)
                |> ignore

                mailbox.Sender() <! JoinFinish
            | JoinJob taskInfo -> 
                let sendUpdateRowMessage (index) = 
                    let rowInfo: RowInfo = {
                        RowIndex = index;
                        RowData = new List<int>(routingTable.[index, *]);
                    }
                    let nodeName = "/user/Worker" + (taskInfo.RequestTo |> string)
                    (select nodeName system) <! UpdateRow rowInfo
                
                let idInBaseVal = toBase4StringTruncated(id, maxRows, baseNumber)
                let toIdInBaseVal = toBase4StringTruncated(taskInfo.RequestTo, maxRows, baseNumber)
                let nonMatchingIndex = checkPrefix(idInBaseVal, toIdInBaseVal)
                if (taskInfo.HopCount = -1 && nonMatchingIndex > 0) then
                    [0 .. (nonMatchingIndex - 1)]
                    |> List.iter (fun i ->  sendUpdateRowMessage(i))
                    |> ignore
                sendUpdateRowMessage(nonMatchingIndex)

                let sendUpdateNeighborMessage() = 
                    let nodeSet = new List<int>()
                    nodeSet.Add(id)
                    for node in SmallerLeafs do nodeSet.Add(node)
                    for node in LargerLeafs do nodeSet.Add(node)
                    let nodeName = "/user/Worker" + (taskInfo.RequestTo |> string)
                    let neighborInfo: NeighborInfo = { WorkerIdList = nodeSet; }
                    (select nodeName system) <! UpdateNeighborSet neighborInfo

                let nextJobInfo: Job = {
                    RequestFrom = taskInfo.RequestFrom;
                    RequestTo = taskInfo.RequestTo;
                    HopCount = taskInfo.HopCount + 1;
                }

                if ((SmallerLeafs.Count > 0 && taskInfo.RequestTo >= (SmallerLeafs |> Seq.min) && taskInfo.RequestTo <= id) || 
                    (LargerLeafs.Count > 0 && taskInfo.RequestTo <= (LargerLeafs |> Seq.max) && taskInfo.RequestTo >= id)) then
                    let mutable diff = idSpace + 10
                    let mutable nearest = -1
                    if (taskInfo.RequestTo < id) then
                        for node in SmallerLeafs do
                            if (abs (taskInfo.RequestTo - node) < diff) then
                                nearest <- node
                                diff <- abs (taskInfo.RequestTo - node)
                    else
                        for node in LargerLeafs do
                            if (abs (taskInfo.RequestTo - node) < diff) then
                                nearest <- node
                                diff <- abs (taskInfo.RequestTo - node)

                    if (abs (taskInfo.RequestTo - id) > diff) then
                        let nodeName = "/user/Worker" + (nearest |> string)
                        (select nodeName system) <! JoinJob nextJobInfo
                    else
                        sendUpdateNeighborMessage()
                else if (SmallerLeafs.Count > 0 && SmallerLeafs.Count < baseNumber && taskInfo.RequestTo < (SmallerLeafs |> Seq.min)) then
                    let nodeName = "/user/Worker" + ((SmallerLeafs |> Seq.min) |> string)
                    (select nodeName system) <! JoinJob nextJobInfo
                else if (LargerLeafs.Count > 0 && LargerLeafs.Count < baseNumber && taskInfo.RequestTo > (LargerLeafs |> Seq.max)) then
                    let nodeName = "/user/Worker" + ((LargerLeafs |> Seq.max) |> string)
                    (select nodeName system) <! JoinJob nextJobInfo
                else if ((SmallerLeafs.Count = 0 && taskInfo.RequestTo < id) || (LargerLeafs.Count = 0 && taskInfo.RequestTo > id)) then
                    sendUpdateNeighborMessage()
                else if (routingTable.[nonMatchingIndex, (toIdInBaseVal.[nonMatchingIndex] |> string |> int)] <> -1) then
                    let nodeName = "/user/Worker" + ((routingTable.[nonMatchingIndex, (toIdInBaseVal.[nonMatchingIndex] |> string |> int)]) |> string)
                    (select nodeName system) <! JoinJob nextJobInfo
                else if (taskInfo.RequestTo > id) then
                    let nodeName = "/user/Worker" + ((LargerLeafs |> Seq.max) |> string)
                    (select nodeName system) <! JoinJob nextJobInfo
                else if (taskInfo.RequestTo < id) then
                    let nodeName = "/user/Worker" + ((SmallerLeafs |> Seq.min) |> string)
                    (select nodeName system) <! JoinJob nextJobInfo
                else ()
            | RouteJob taskInfo -> 
                if (taskInfo.RequestTo = id) then
                    boss <! FinishRoute { NumberOfHops = taskInfo.HopCount + 1; }
                else
                    let nextJobInfo: Job = {
                        RequestFrom = taskInfo.RequestFrom;
                        RequestTo = taskInfo.RequestTo;
                        HopCount = taskInfo.HopCount + 1;
                    }

                    let idInBaseVal = toBase4StringTruncated(id, maxRows, baseNumber)
                    let toIdInBaseVal = toBase4StringTruncated(taskInfo.RequestTo, maxRows, baseNumber)
                    let nonMatchingIndex = checkPrefix(idInBaseVal, toIdInBaseVal)
                    if ((SmallerLeafs.Count > 0 && taskInfo.RequestTo >= (SmallerLeafs |> Seq.min) && taskInfo.RequestTo < id) || 
                        (LargerLeafs.Count > 0 && taskInfo.RequestTo <= (LargerLeafs |> Seq.max) && taskInfo.RequestTo > id)) then
                        let mutable diff = idSpace + 10
                        let mutable nearest = -1
                        if (taskInfo.RequestTo < id) then
                            for node in SmallerLeafs do
                                if (abs (taskInfo.RequestTo - node) < diff) then
                                    nearest <- node
                                    diff <- abs (taskInfo.RequestTo - node)
                        else
                            for node in LargerLeafs do
                                if (abs (taskInfo.RequestTo - node) < diff) then
                                    nearest <- node
                                    diff <- abs (taskInfo.RequestTo - node)

                        if (abs (taskInfo.RequestTo - id) > diff) then
                            let nodeName = "/user/Worker" + (nearest |> string)
                            (select nodeName system) <! RouteJob nextJobInfo
                        else
                            boss <! FinishRoute { NumberOfHops = taskInfo.HopCount + 1; }
                    else if (SmallerLeafs.Count > 0 && SmallerLeafs.Count < baseNumber && taskInfo.RequestTo < (SmallerLeafs |> Seq.min)) then
                        let nodeName = "/user/Worker" + ((SmallerLeafs |> Seq.min) |> string)
                        (select nodeName system) <! RouteJob nextJobInfo
                    else if (LargerLeafs.Count > 0 && LargerLeafs.Count < baseNumber && taskInfo.RequestTo > (LargerLeafs |> Seq.max)) then
                        let nodeName = "/user/Worker" + ((LargerLeafs |> Seq.max) |> string)
                        (select nodeName system) <! RouteJob nextJobInfo
                    else if ((SmallerLeafs.Count = 0 && taskInfo.RequestTo < id) || (LargerLeafs.Count = 0 && taskInfo.RequestTo > id)) then
                        boss <! FinishRoute { NumberOfHops = taskInfo.HopCount + 1; }
                    else if (routingTable.[nonMatchingIndex, (toIdInBaseVal.[nonMatchingIndex] |> string |> int)] <> -1) then
                        let nodeName = "/user/Worker" + ((routingTable.[nonMatchingIndex, (toIdInBaseVal.[nonMatchingIndex] |> string |> int)]) |> string)
                        (select nodeName system) <! RouteJob nextJobInfo
                    else if (taskInfo.RequestTo > id) then
                        let nodeName = "/user/Worker" + ((LargerLeafs |> Seq.max) |> string)
                        (select nodeName system) <! RouteJob nextJobInfo
                        boss <! RouteToWorkerNotFound
                    else if (taskInfo.RequestTo < id) then
                        let nodeName = "/user/Worker" + ((SmallerLeafs |> Seq.min) |> string)
                        (select nodeName system) <! RouteJob nextJobInfo
                        boss <! RouteToWorkerNotFound
                    else ()
            | UpdateRow rowInfo -> 
                let updateRoutingTableEntry (row, col, value) = 
                    if (routingTable.[row, col] = -1) then
                        routingTable.[row, col] <- value
                    else ()

                [0 .. baseNumber - 1]
                |> List.iter (fun i ->  updateRoutingTableEntry (rowInfo.RowIndex, i, rowInfo.RowData.[i]))
                |> ignore
            | UpdateNeighborSet neighborInfo -> 
                updateLeafSet (neighborInfo.WorkerIdList)

                let idInBaseVal = toBase4StringTruncated(id, maxRows, baseNumber)
                updateSamePrefixTableEntries(idInBaseVal, neighborInfo.WorkerIdList)

                for node in SmallerLeafs do 
                    backNumber <- backNumber + 1
                    let nodeName = "/user/Worker" + (node |> string)
                    (select nodeName system) <! SendAcknowledgementToBoss { NewWorkerId = id; }

                for node in LargerLeafs do 
                    backNumber <- backNumber + 1
                    let nodeName = "/user/Worker" + (node |> string)
                    (select nodeName system) <! SendAcknowledgementToBoss { NewWorkerId = id; }
                
                for i in [0 .. (maxRows - 1)] do
                    for j in [0 .. (baseNumber - 1)] do
                        if (routingTable.[i, j] <> -1) then
                            backNumber <- backNumber + 1
                            let nodeName = "/user/Worker" + (routingTable.[i, j] |> string)
                            (select nodeName system) <! SendAcknowledgementToBoss { NewWorkerId = id; }

                let idInBaseVal = toBase4StringTruncated(id, maxRows, baseNumber)
                let col(row: int) = idInBaseVal.[row] |> string |> int
                [0 .. (maxRows - 1)]
                |> List.iter(fun row -> routingTable.[row, col(row)] <- id)
                |> ignore
            | SendAcknowledgementToBoss ackInfo -> 
                let tempList = new List<int>()
                tempList.Add(ackInfo.NewWorkerId)
                updateLeafSet(tempList)
                let idInBaseVal = toBase4StringTruncated(id, maxRows, baseNumber)
                updateSamePrefixTableEntries(idInBaseVal, tempList)
                mailbox.Sender() <! Acknowledgement
            | Acknowledgement -> 
                backNumber <- backNumber - 1
                if backNumber = 0 then 
                    boss <! JoinFinish
            | StartRouting routingInfo -> 
                for i in [1 .. routingInfo.RequestCount] do
                    Thread.Sleep(200)
                    let taskInfo: Job = {
                        RequestFrom = id;
                        RequestTo = Random().Next(idSpace)
                        HopCount = -1;
                    }
                    mailbox.Self <! RouteJob taskInfo
            | _ -> ()
        
        return! loop()
    }
    loop()

let Boss (mailbox: Actor<_>) = 
    let mutable totalNumWorkers: int = 0
    let mutable numberOfRequests: int = 0
    
    let mutable numberOfWorkersJoined: int = 0
    let mutable numberOfWorkersRouted: int = 0
    let mutable numberOfRouteNotFound: int = 0
    let mutable numberOfWorkersNotInBoth: int = 0

    let mutable numberOfHops: int = 0

    let mutable maxRows: int = 0
    let mutable maxWorkers: int = 0

    let nodeList = new List<int>()
    let groupOne = new List<int>()

    let mutable systemRef = null

    let initWorkerList () = 
        let random = Random()
        let randomShuffle () =
            [0 .. (maxWorkers - 2)]
            |> List.iter (fun i ->  let j = random.Next(i, maxWorkers)
                                    let temp = nodeList.[i]
                                    nodeList.[i] <- nodeList.[j]
                                    nodeList.[j] <- temp)
            |> ignore
        
        [0 .. (maxWorkers - 1)]
        |> List.iter(fun i ->   nodeList.Add(i))
        |> ignore
        randomShuffle ()

    let initWorkers () = 
        [0 .. (totalNumWorkers - 1)]
        |> List.iter (fun i ->  let name = "Worker" + (nodeList.[i] |> string)
                                let node = spawn system name Worker
                                let initMessage: StartWorker = {
                                    Id = nodeList.[i];
                                    MaxRows = maxRows;
                                }
                                node <! StartWorker initMessage)
        |> ignore

    let initWork () =
        let firstWorkerName = "/user/Worker" + (nodeList.[0] |> string)
        let addFirstWorkerMessage: AddFirstWorker = {
            WorkerGroup = new List<int>(groupOne);
        }
        (select firstWorkerName system) <! AddFirstWorker addFirstWorkerMessage
        
    let rec loop() = actor {
        let! message = mailbox.Receive()

        match message with 
            | StartBoss initMessage -> 
                systemRef <- mailbox.Sender()
                totalNumWorkers <- initMessage.NumWorkers
                numberOfRequests <- initMessage.NumRequests
                maxRows <- (ceil ((Math.Log(float initMessage.NumWorkers))/(Math.Log(float(baseNumber))))) |> int
                maxWorkers <- Math.Pow (float baseNumber, float maxRows) |> int
                initWorkerList()
                groupOne.Add(nodeList.[0])
                initWorkers()
                initWork()
            | JoinFinish -> 
                numberOfWorkersJoined <- numberOfWorkersJoined + 1
                if (numberOfWorkersJoined = totalNumWorkers) then
                    mailbox.Self <! StartRoutingBoss
                else
                    mailbox.Self <! JoinWorkersInDT
            | JoinWorkersInDT -> 
                let nodeId = nodeList.[Random().Next(numberOfWorkersJoined)]
                let nodeName = "/user/Worker" + (nodeId |> string)
                let node = select nodeName system
                let taskInfo: Job = {
                    RequestFrom = nodeId;
                    RequestTo = nodeList.[numberOfWorkersJoined];
                    HopCount = -1;
                }
                node <! JoinJob taskInfo
            | StartRoutingBoss -> 
                (select "/user/Worker*" system) <! StartRouting { WorkerCount = totalNumWorkers; RequestCount = numberOfRequests; }
            | FinishRoute finishRouteMessage -> 
                numberOfWorkersRouted <- numberOfWorkersRouted + 1
                numberOfHops <- numberOfHops + finishRouteMessage.NumberOfHops
                if (numberOfWorkersRouted >= totalNumWorkers * numberOfRequests) then
                    let message = "Work completed." + 
                                    "\nTotal Number of Routes: " + (numberOfWorkersRouted |> string) + 
                                    "\nTotal Number of Hops: " + (numberOfHops |> string) + 
                                    "\nAverage Number of Hops: " + 
                                    ((float numberOfHops / float numberOfWorkersRouted) |> string)
                    systemRef <! message
            | WorkerNotFound -> 
                numberOfWorkersNotInBoth <- numberOfWorkersNotInBoth + 1
            | RouteToWorkerNotFound -> 
                numberOfRouteNotFound <- numberOfRouteNotFound + 1
            | _ -> ()
        
        return! loop()
    }
    loop()

let main (numNodes, numRequests) = 
    boss <- spawn system "Boss" Boss
    let response = Async.RunSynchronously(boss <? StartBoss { NumWorkers = numNodes; NumRequests = numRequests; })
    printfn "%A" response
    system.Terminate() |> ignore

main(args.[3]|>int, args.[4]|>int)