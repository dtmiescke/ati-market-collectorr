import crypto from "node:crypto";
const required=["ATI_INGEST_URL","ATI_STREAM_INGEST_SECRET","ALPACA_API_KEY","ALPACA_API_SECRET"];
for(const k of required) if(!process.env[k]) throw new Error("Missing env: "+k);
const ingestUrl=process.env.ATI_INGEST_URL,secret=process.env.ATI_STREAM_INGEST_SECRET;
const symbols=(process.env.ATI_SYMBOLS||"SPY,QQQ,IWM,DIA,AAPL,MSFT,NVDA,AMZN,META,GOOGL,TSLA,AMD").split(",").map(x=>x.trim().toUpperCase()).filter(Boolean);
const collectorId=process.env.ATI_COLLECTOR_ID||"fly-primary";
let sequence=Number(process.env.ATI_START_SEQUENCE||0),socket,stopped=false,retry=1000; const buckets=new Map();
function addTrade(t){const price=Number(t.p),size=Math.max(0,Number(t.s)||0),ts=Date.parse(t.t);if(!t.S||!Number.isFinite(price)||price<=0||!Number.isFinite(ts))return;const second=Math.floor(ts/1000)*1000,key=t.S+"|"+second,b=buckets.get(key);if(b){b.high=Math.max(b.high,price);b.low=Math.min(b.low,price);b.close=price;b.size+=size}else buckets.set(key,{symbol:t.S,open:price,high:price,low:price,close:price,size,timestampMs:second,source:"ALPACA_IEX"})}
async function flush(){const cutoff=Date.now()-1000,ticks=[];for(const [k,b] of buckets)if(b.timestampMs<=cutoff){ticks.push({...b,price:b.close});buckets.delete(k)}if(!ticks.length)return;const batch={collectorId,sequence:++sequence,sentAt:new Date().toISOString(),ticks},raw=JSON.stringify(batch),signature=crypto.createHmac("sha256",secret).update(raw).digest("hex");const r=await fetch(ingestUrl,{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({raw,signature})});if(!r.ok)throw new Error("ATI ingest HTTP "+r.status)}
function connect(){socket=new WebSocket("wss://stream.data.alpaca.markets/v2/iex");socket.onopen=()=>{retry=1000;socket.send(JSON.stringify({action:"auth",key:process.env.ALPACA_API_KEY,secret:process.env.ALPACA_API_SECRET}))};socket.onmessage=e=>{let xs;try{xs=JSON.parse(e.data)}catch{return}for(const x of xs){if(x.T==="success"&&x.msg==="authenticated")socket.send(JSON.stringify({action:"subscribe",trades:symbols}));else if(x.T==="t")addTrade(x)}};socket.onerror=()=>socket.close();socket.onclose=()=>{if(!stopped)setTimeout(connect,retry);retry=Math.min(retry*2,30000)}}
setInterval(()=>flush().catch(e=>console.error(new Date().toISOString(),"flush",e.message)),1000);
setInterval(()=>console.log(new Date().toISOString(),"healthy",{symbols:symbols.length,buffer:buckets.size,sequence}),60000);
process.on("SIGTERM",()=>{stopped=true;socket?.close();process.exit(0)});connect();
