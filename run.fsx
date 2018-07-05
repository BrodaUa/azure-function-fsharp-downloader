#r "System.Net.Http"
#r "System.Drawing"
#r "System.IO"
#r "FSharp.Data"
#r "Microsoft.Azure.WebJobs"

open System
open System.IO
open System.Net
open System.Net.Http
open System.Drawing
open System.Drawing.Imaging
open System.Threading.Tasks
open System.Collections.Generic

open FSharp.Data
open Microsoft.Azure.WebJobs;

type LimitAgentMessage = 
  | Start of Async<unit>
  | Finished

type Microsoft.FSharp.Control.AsyncBuilder with
  member x.Bind(t:Task<'T>, f:'T -> Async<'R>) : Async<'R>  = 
    async.Bind(Async.AwaitTask t, f)

let Run(req: HttpRequestMessage, inBlob:Stream, binder: Binder) =
    async {

        let threadingLimitAgent limit = MailboxProcessor.Start(fun inbox -> async {
            let queue = Queue<_>()
            let count = ref 0
            while true do
                let! msg = inbox.Receive()  
                match msg with 
                | Start work -> queue.Enqueue(work)
                | Finished -> decr count
                if count.Value < limit && queue.Count > 0 then
                    incr count
                    let work = queue.Dequeue()
                    Async.Start(async { 
                        do! work
                        inbox.Post(Finished) }) })

        let processImage (row: CsvRow) =
            async {
                let name, path = row.[0], row.[1]
                try
                    let wc = new WebClient()
                    use! reader = wc.OpenReadTaskAsync(path)
                    use! writer = binder.BindAsync<Stream>(new BlobAttribute("container-name/images/" + name + ".jpg", FileAccess.Write))
                    Image.FromStream(reader, true).Save(writer, ImageFormat.Jpeg)
                with
                | ex -> 
                    use! exWriter = binder.BindAsync<TextWriter>(new BlobAttribute("container-name/exceptions/" + name + ".txt", FileAccess.Write))
                    exWriter.WriteLine(ex.Message)
                    exWriter.WriteLine(ex.StackTrace)
            } 
        
        let agent = threadingLimitAgent 32
        let train = CsvFile.Load(inBlob)
        
        for row in train.Rows do
            agent.Post(Start(processImage row))

        return req.CreateResponse(HttpStatusCode.OK, "mapping queued");

    } |> Async.RunSynchronously