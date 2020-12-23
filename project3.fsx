#r "nuget: Akka.FSharp"

open Akka.Actor
open Akka.FSharp
open System
open System.Linq
open System.Numerics
open System.Globalization
open System.Collections.Generic

let system = System.create "System" (Configuration.defaultConfig())
let args = System.Environment.GetCommandLineArgs()

type BossInitMessage = {
    NumNodes: int;
    NumRequests: int;
}

type BossCompletionMessage = {
    BossCompleted: bool;
}

type WorkerInitMessage = {
    NumNodes: int;
    NumRequests: int;
    MyId: int;
    BaseNumber: int;
    GroupOne: List<int>;
}

type FinishedJoining = {
    FinishedJoining: bool;
}

type StartRouting = {
    StartRouting: bool;
}

type SecondaryJoin = {
    SecondaryJoin: bool;
}

type Route = {
    Msg: string;
    RequestFrom: int;
    RequestTo: int;
    Hops: int;
}

type RouteFinish = {
    RequestFrom: int;
    RequestTo: int;
    Hops: int;
}

type NotInBoth = {
    NotInBoth: bool;
}

type RoutedNotInBoth = {
    RoutedNotInBoth: bool;
}

type AddRow = {
    RowNum: int;
    NewRow: List<int>;
}

type AddLeaf = {
    AllLeaf: List<int>;
}

type Update = {
    NewNodeId: int;
}

type Acknowledgement = {
    Acknowledgement: bool;
}

type DisplayLeafAndRouting = {
    Display: bool;
}

let md5 (msg: string) =
    use md5 = System.Security.Cryptography.MD5.Create()
    msg
    |> System.Text.Encoding.ASCII.GetBytes
    |> md5.ComputeHash
    |> Seq.map (fun x -> x.ToString("X2"))
    |> Seq.reduce ( + )

let hex2dec (s:string) =
    BigInteger.Parse("0" + s, NumberStyles.AllowHexSpecifier)

let hashDecTuple (s:string) =
    let hash = md5(s)
    (hash, hex2dec(hash))

let generateNodeIds (N:int) = 
    [for i in [1..N] do yield hashDecTuple(string i)]
    |> List.sortBy (fun (x : System.Tuple<string, bigint>) -> snd(x))

let divideByLog4 (N: float) =
    N/Math.Log(4.0)

let getBaseNumber (N:int) = 
    N
    |> float
    |> Math.Log
    |> divideByLog4
    |> ceil
    |> int

let getPowOf4 (N:int) =
    4.0**(float N)
    |> int

let shuffle (L:List<int>) =
    let random = new Random()
    let n = L.Count
    for i in n-1 .. -1 .. 2 do
        let rnd = random.Next(i+1)
        let value = L.[rnd]
        L.[rnd] <- L.[i]
        L.[i] <- value
    L

let generateInitList =
    let mutable list = new List<int>()
    for i in 1..4 do
        list.Add(-1)
    list

let intToDigits baseN value : int list =
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
    |> intToDigits 4
    |> List.fold (fun acc x -> acc + x.ToString("X")) ""
    |> string

let toBase4StringTruncated(raw, length) =
    let mutable string = toBase4String raw
    let diff = length - string.Length
    if diff > 0 then
        let mutable j = 0
        while (j<diff) do
            string <- "0" + string
            j <- j+1
    string

let rec checkPrefix(s1, s2) =
    if s1 <> "" && s2 <> "" && s1.[0] = s2.[0] then
        1 + checkPrefix (s1.[1..], s2.[1..])
    else
        0

type Worker() =
    inherit Actor()
    let mutable numNodes = 0
    let mutable numRequests = 0
    let mutable myId = 0
    let mutable baseNumber = 0
    let mutable groupOne: List<int> = null
    let mutable smallerLeaf = new List<int>()
    let mutable largerLeaf = new List<int>()
    let mutable idSpace = 0
    let mutable numOfBack = 0
    let mutable routingTable = new List<List<int>>()

    let addNode(node: int) =
        if (node>myId && not(largerLeaf.Contains(node))) then
            if (largerLeaf.Count < 4) then
                largerLeaf.Add(node)
            else
                if node<largerLeaf.Max() then
                    largerLeaf.Remove(largerLeaf.Max()) |> ignore
                    largerLeaf.Add(node)
        else if(node<myId && not(smallerLeaf.Contains(node))) then
            if (smallerLeaf.Count < 4) then
                smallerLeaf.Add(node)
            else
                if node>smallerLeaf.Min() then
                    smallerLeaf.Remove(smallerLeaf.Min()) |> ignore
                    smallerLeaf.Add(node)
        let samePrefix = checkPrefix(toBase4StringTruncated(myId, baseNumber), toBase4StringTruncated(node, baseNumber))
        if (routingTable.[samePrefix].[toBase4StringTruncated(node, baseNumber).[samePrefix]|>string|>int] = -1) then
            routingTable.[samePrefix].[toBase4StringTruncated(node, baseNumber).[samePrefix]|>string|>int] <- node

    let addBuffer(all: List<int>) =
        for i in 0 .. all.Count-1 do
            addNode(all.[i])

    let display() =
        printfn "smallllerLeaf:%A" smallerLeaf
        //printfn "largerLeaf:%A" largerLeaf
        for i in 0 .. baseNumber-1 do
            let mutable j = 0
            printfn "Row %d" i
            for j in 0..3 do
                printf "%d " routingTable.[i].[j]
                
    override x.OnReceive (message:obj) = 
        match message with
        | :? WorkerInitMessage as msg ->
            numNodes <- msg.NumNodes
            numRequests <- msg.NumRequests
            myId <- msg.MyId
            baseNumber <- msg.BaseNumber
            groupOne <- msg.GroupOne |> List<int>
            idSpace <- getPowOf4 baseNumber
            for i in 0 .. baseNumber-1 do
                routingTable.Add(generateInitList)
            groupOne.Remove(myId) |> ignore
            addBuffer(groupOne)
            for i in 0 .. baseNumber-1 do
                routingTable.[i].[toBase4StringTruncated(myId, baseNumber).[i] |> string |> int] <- myId
            x.Sender.Tell { FinishedJoining = true }
        | :? StartRouting as msg ->
            for i in 1 .. numRequests do
                let random = new System.Random()
                system.Scheduler.ScheduleTellOnce(200, x.Self, { 
                    Msg = "Route"; RequestFrom = myId; RequestTo = random.Next(idSpace); Hops = -1 
                }, x.Self)
        | :? Route as msg ->
            if msg.Msg = "Join" then
                let samePrefix = checkPrefix(toBase4StringTruncated(myId, baseNumber), toBase4StringTruncated(msg.RequestTo, baseNumber))
                if (msg.Hops = -1 && samePrefix > 0) then
                    for i in 0 .. samePrefix-1 do
                        let mutable rowCopy = new List<int>(routingTable.[i].ToArray())
                        system.ActorSelection("akka://System/user/Worker" + string msg.RequestTo).Tell(
                            { RowNum = i; NewRow = rowCopy })
                let mutable rowCopy = new List<int>(routingTable.[samePrefix].ToArray())
                system.ActorSelection("akka://System/user/Worker" + string msg.RequestTo).Tell(
                    { RowNum = samePrefix; NewRow = rowCopy })
                if ((smallerLeaf.Count > 0 && msg.RequestTo >= smallerLeaf.Min() && msg.RequestTo <= myId) ||
                    (largerLeaf.Count > 0 && msg.RequestTo <= largerLeaf.Max() && msg.RequestTo >= myId)) then
                    let mutable diff = idSpace + 10
                    let mutable nearest = -1
                    if (msg.RequestTo < myId) then
                        for i in 0 .. smallerLeaf.Count-1 do
                            if (abs(msg.RequestTo - smallerLeaf.[i]) < diff) then
                                nearest <- smallerLeaf.[i]
                                diff <- abs(msg.RequestTo - smallerLeaf.[i])
                    else
                        for i in 0 .. largerLeaf.Count-1 do
                            if (abs(msg.RequestTo - largerLeaf.[i]) < diff) then
                                nearest <- largerLeaf.[i]
                                diff <- abs(msg.RequestTo - largerLeaf.[i])
                    if (abs(msg.RequestTo - myId) > diff) then
                        system.ActorSelection("akka://System/user/Worker" + string nearest).Tell(
                            { Msg = msg.Msg; RequestFrom = msg.RequestFrom; RequestTo = msg.RequestTo; Hops = msg.Hops+1 })
                    
                        let mutable allLeaf = new List<int>()
                        allLeaf <- smallerLeaf.Concat(largerLeaf).ToList()
                        allLeaf.Insert(0, myId)
                        system.ActorSelection("akka://System/user/Worker" + string msg.RequestTo).Tell(
                            { AllLeaf = allLeaf })
                else if (smallerLeaf.Count < 4 && smallerLeaf.Count > 0 && msg.RequestTo < smallerLeaf.Min()) then
                    system.ActorSelection("akka://System/user/Worker" + string(smallerLeaf.Min())).Tell(
                        { Msg = msg.Msg; RequestFrom = msg.RequestFrom; RequestTo = msg.RequestTo; Hops = msg.Hops+1 })
                else if (largerLeaf.Count < 4 && largerLeaf.Count > 0 && msg.RequestTo > largerLeaf.Max()) then
                    system.ActorSelection("akka://System/user/Worker" + string(largerLeaf.Max())).Tell(
                        { Msg = msg.Msg; RequestFrom = msg.RequestFrom; RequestTo = msg.RequestTo; Hops = msg.Hops+1 })
                else if ((smallerLeaf.Count = 0 && msg.RequestTo < myId) || (largerLeaf.Count = 0 && msg.RequestTo > myId)) then
                    let mutable allLeaf = new List<int>()
                    allLeaf <- smallerLeaf.Concat(largerLeaf).ToList()
                    allLeaf.Insert(0, myId)
                    system.ActorSelection("akka://System/user/Worker" + string msg.RequestTo).Tell(
                        { AllLeaf = allLeaf })
                else if (routingTable.[samePrefix].[toBase4StringTruncated(msg.RequestTo, baseNumber).[samePrefix]|>string|>int] <> -1) then
                    system.ActorSelection("akka://System/user/Worker" + string(routingTable.[samePrefix].[toBase4StringTruncated(msg.RequestTo, baseNumber).[samePrefix]|>string|>int])).Tell(
                        { Msg = msg.Msg; RequestFrom = msg.RequestFrom; RequestTo = msg.RequestTo; Hops = msg.Hops+1 })
                else if msg.RequestTo > myId then
                    system.ActorSelection("akka://System/user/Worker" + string(largerLeaf.Max())).Tell(
                        { Msg = msg.Msg; RequestFrom = msg.RequestFrom; RequestTo = msg.RequestTo; Hops = msg.Hops+1 })
                    system.ActorSelection("akka://System/user/Boss").Tell{ NotInBoth = true }
                else if msg.RequestTo < myId then
                    system.ActorSelection("akka://System/user/Worker" + string(smallerLeaf.Min())).Tell(
                        { Msg = msg.Msg; RequestFrom = msg.RequestFrom; RequestTo = msg.RequestTo; Hops = msg.Hops+1 })
                    system.ActorSelection("akka://System/user/Boss").Tell{ NotInBoth = true }
                else
                    printfn "Not possible"
            else if msg.Msg = "Route" then
                if myId = msg.RequestTo then
                    system.ActorSelection("akka://System/user/Boss").Tell(
                        { RequestFrom = msg.RequestFrom; RequestTo = msg.RequestTo; Hops = msg.Hops+1 })
                else
                    let samePrefix = checkPrefix(toBase4StringTruncated(myId, baseNumber), toBase4StringTruncated(msg.RequestTo, baseNumber))
                    if ((smallerLeaf.Count > 0 && msg.RequestTo >= smallerLeaf.Min() && msg.RequestTo < myId) ||
                        (largerLeaf.Count > 0 && msg.RequestTo <= largerLeaf.Max() && msg.RequestTo > myId)) then
                        let mutable diff = idSpace + 10
                        let mutable nearest = -1
                        if (msg.RequestTo < myId) then
                            for i in 0 .. smallerLeaf.Count-1 do
                                if (abs(msg.RequestTo - smallerLeaf.[i]) < diff) then
                                    nearest <- smallerLeaf.[i]
                                    diff <- abs(msg.RequestTo - smallerLeaf.[i])
                        else
                            for i in 0 .. largerLeaf.Count-1 do
                                if (abs(msg.RequestTo - largerLeaf.[i]) < diff) then
                                    nearest <- largerLeaf.[i]
                                    diff <- abs(msg.RequestTo - largerLeaf.[i])
                        if (abs(msg.RequestTo - myId) > diff) then
                            system.ActorSelection("akka://System/user/Worker" + string nearest).Tell(
                                { Msg = msg.Msg; RequestFrom = msg.RequestFrom; RequestTo = msg.RequestTo; Hops = msg.Hops+1 })
                        else
                            system.ActorSelection("akka://System/user/Boss").Tell(
                                { RequestFrom = msg.RequestFrom; RequestTo = msg.RequestTo; Hops = msg.Hops+1 })
                    else if (smallerLeaf.Count < 4 && smallerLeaf.Count > 0 && msg.RequestTo < smallerLeaf.Min()) then
                        system.ActorSelection("akka://System/user/Worker" + string(smallerLeaf.Min())).Tell(
                            { Msg = msg.Msg; RequestFrom = msg.RequestFrom; RequestTo = msg.RequestTo; Hops = msg.Hops+1 })
                    else if (largerLeaf.Count < 4 && largerLeaf.Count > 0 && msg.RequestTo > largerLeaf.Max()) then
                        system.ActorSelection("akka://System/user/Worker" + string(largerLeaf.Max())).Tell(
                            { Msg = msg.Msg; RequestFrom = msg.RequestFrom; RequestTo = msg.RequestTo; Hops = msg.Hops+1 })
                    else if ((smallerLeaf.Count = 0 && msg.RequestTo < myId) || (largerLeaf.Count = 0 && msg.RequestTo > myId)) then
                        system.ActorSelection("akka://System/user/Boss").Tell(
                            { RequestFrom = msg.RequestFrom; RequestTo = msg.RequestTo; Hops = msg.Hops+1 })
                    else if (routingTable.[samePrefix].[toBase4StringTruncated(msg.RequestTo, baseNumber).[samePrefix]|>string|>int] <> -1) then
                        system.ActorSelection("akka://System/user/Worker" + string(routingTable.[samePrefix].[toBase4StringTruncated(msg.RequestTo, baseNumber).[samePrefix]|>string|>int])).Tell(
                            { Msg = msg.Msg; RequestFrom = msg.RequestFrom; RequestTo = msg.RequestTo; Hops = msg.Hops+1 })
                    else if msg.RequestTo > myId then
                        system.ActorSelection("akka://System/user/Worker" + string(largerLeaf.Max())).Tell(
                            { Msg = msg.Msg; RequestFrom = msg.RequestFrom; RequestTo = msg.RequestTo; Hops = msg.Hops+1 })
                        system.ActorSelection("akka://System/user/Boss").Tell{ NotInBoth = true }
                    else if msg.RequestTo < myId then
                        system.ActorSelection("akka://System/user/Worker" + string(smallerLeaf.Min())).Tell(
                            { Msg = msg.Msg; RequestFrom = msg.RequestFrom; RequestTo = msg.RequestTo; Hops = msg.Hops+1 })
                        system.ActorSelection("akka://System/user/Boss").Tell{ NotInBoth = true }
                    else
                        printfn "Not possible"
        | :? AddRow as msg ->
            for i in 0 .. 3 do
                if routingTable.[msg.RowNum].[i] = -1 then
                    routingTable.[msg.RowNum].[i] <- (msg.NewRow|>List<int>).[i]
        | :? AddLeaf as msg ->
            addBuffer(msg.AllLeaf|>List<int>)
            for i in 0 .. smallerLeaf.Count-1 do
                numOfBack <- numOfBack + 1
                system.ActorSelection("akka://System/user/Worker" + string smallerLeaf.[i]).Tell { NewNodeId = myId }
            for i in 0 .. largerLeaf.Count-1 do
                numOfBack <- numOfBack + 1
                system.ActorSelection("akka://System/user/Worker" + string largerLeaf.[i]).Tell { NewNodeId = myId }
            for i in 0 .. baseNumber-1 do
                let mutable j = 0
                for j in 0 .. 3 do
                    if routingTable.[i].[j] <> -1 then
                        numOfBack <- numOfBack + 1
                        system.ActorSelection("akka://System/user/Worker" + string routingTable.[i].[j]).Tell { NewNodeId = myId }
            for i in 0 .. baseNumber-1 do
                routingTable.[i].[toBase4StringTruncated(myId, baseNumber).[i]|>string|>int] <- myId            
        | :? Update as msg ->
            addNode(msg.NewNodeId)
            x.Sender.Tell { Acknowledgement = true }
        | :? Acknowledgement as msg ->
            numOfBack <- numOfBack - 1
            if numOfBack = 0 then
                system.ActorSelection("akka://System/user/Boss").Tell { FinishedJoining = true }
        | :? DisplayLeafAndRouting as msg ->
            display()
        | _ -> printfn "ERROR IN WORKER"

type Boss() =
    inherit Actor()
    let mutable baseNumber = 0
    let mutable numNodes = 0
    let mutable numRequests = 0
    let mutable nodeIdSpace = 0
    let mutable randomList = new List<int>()
    let mutable groupOne = new List<int>()
    let mutable groupOneSize = 0
    let mutable i = -1
    let mutable numHops = 0
    let mutable numJoined = 0
    let mutable numNotInBoth = 0
    let mutable numRouteNotInBoth = 0
    let mutable numRouted = 0
    override x.OnReceive (message:obj) =   
        match message with
        | :? BossInitMessage as msg ->
            numNodes <- msg.NumNodes
            numRequests <- msg.NumRequests
            baseNumber <-  getBaseNumber numNodes
            nodeIdSpace <- getPowOf4 baseNumber
            groupOneSize <- if (numNodes <= 1024) then numNodes else 1024
            for i in 0 .. nodeIdSpace-1 do
                randomList.Add(i)
            randomList <- shuffle randomList
            for i in 0 .. groupOneSize-1 do
                groupOne.Add(randomList.[i])
            let mutable actorArray: list<IActorRef> = [for i in 0 .. numNodes-1 do yield system.ActorOf(Props(typedefof<Worker>), "Worker" + (string) randomList.[i])]
            let mutable groupOneCopy = new List<int>(groupOne)
            for i in 0 .. groupOneSize-1 do
                actorArray.[i].Tell { NumNodes = numNodes; NumRequests = numRequests; MyId = randomList.[i]; BaseNumber = baseNumber; GroupOne = groupOneCopy }
        | :? FinishedJoining as msg ->
            numJoined <- numJoined + 1
            if numJoined = groupOneSize then
                if numJoined >= numNodes then
                    x.Self.Tell { StartRouting = true }
                else
                    x.Self.Tell { SecondaryJoin = true }
            if numJoined > groupOneSize then
                if numJoined = numNodes then
                    x.Self.Tell { StartRouting = true }
                else
                    x.Self.Tell { SecondaryJoin = true }
        | :? StartRouting as msg ->
            printfn "Joined"
            printfn "Routing"
            system.ActorSelection("akka://System/user/Worker*").Tell { StartRouting = true }
        | :? SecondaryJoin as msg ->
            let random = new Random()
            let startId = randomList.[random.Next(numJoined)]
            system.ActorSelection("akka://System/user/Worker" + string startId).Tell(
                { Msg = "Join"; RequestFrom = startId; RequestTo = randomList.[numJoined]; Hops = -1 }        
            )    
        | :? NotInBoth as msg ->
            numNotInBoth <- numNotInBoth + 1
        | :? RoutedNotInBoth as msg ->
            numRouteNotInBoth <- numRouteNotInBoth + 1
        | :? RouteFinish as msg ->
            numRouted <- numRouted + 1
            numHops <- numHops + msg.Hops
            for i in 1 .. 10 do
                if (numRouted = numNodes * numRequests * i / 10) then
                    for j in 1 .. i do
                        printf "."
                    printf "|"
            if (numRouted >= numNodes * numRequests) then
                printfn ""
                printfn "Total Routes -> %d, Total Hops -> %d"  numRouted numHops
                printfn "Average Hops Per Route -> %A" (float(numHops)/float(numRouted))
                x.Self.Tell(PoisonPill.Instance);
        | _ -> printfn "ERROR IN BOSS"

let boss = system.ActorOf(Props(typedefof<Boss>), "Boss")
// system.Terminate()
let (task:Async<BossCompletionMessage>) = (boss <? { NumNodes = args.[3]|>int; NumRequests = args.[4]|>int; })
let response = Async.RunSynchronously (task)
boss.Tell(PoisonPill.Instance);