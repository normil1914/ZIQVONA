const http=require('http'),fs=require('fs'),path=require('path');
const WebSocket=require('ws');
const PORT=process.env.PORT||3000;
const users=new Map(), clients=new Map();
const server=http.createServer((req,res)=>{let p=req.url.split('?')[0];if(p==='/')p='/index.html';const f=path.join(__dirname,'public',path.normalize(p).replace(/^\.\.[\\/]+/,''));fs.readFile(f,(e,d)=>{if(e){res.writeHead(404);return res.end('Not found')}const t={'.html':'text/html','.js':'text/javascript','.css':'text/css'}[path.extname(f)]||'application/octet-stream';res.writeHead(200,{'Content-Type':t});res.end(d)})});
const wss=new WebSocket.Server({server});
function send(ws,o){if(ws&&ws.readyState===1)ws.send(JSON.stringify(o))}
function presence(){return [...users.keys()]}
wss.on('connection',ws=>{
 send(ws,{type:'welcome'});
 ws.on('message',raw=>{let m;try{m=JSON.parse(raw)}catch{return}
  if(m.type==='login'){let u=String(m.username||'').trim().slice(0,30);if(!u)return send(ws,{type:'error',message:'Mete yon non.'});if(users.has(u)&&users.get(u)!==ws)return send(ws,{type:'error',message:'Non sa deja konekte.'});users.set(u,ws);clients.set(ws,u);send(ws,{type:'login_ok',username:u,online:presence()});wss.clients.forEach(c=>send(c,{type:'presence',online:presence()}));}
  if(m.type==='message'){let from=clients.get(ws),to=String(m.to||''),text=String(m.text||'').trim().slice(0,2000);if(!from||!to||!text)return;let packet={type:'message',id:Date.now()+'-'+Math.random(),from,to,text,time:new Date().toISOString()};send(ws,packet);send(users.get(to),packet)}
  if(m.type==='typing'){let from=clients.get(ws),to=String(m.to||'');if(from&&users.has(to))send(users.get(to),{type:'typing',from})}
  if(m.type==='call-offer'||m.type==='call-answer'||m.type==='ice'||m.type==='call-end'){let to=String(m.to||''),from=clients.get(ws);if(from&&users.has(to))send(users.get(to),{...m,from})}
 });
 ws.on('close',()=>{let u=clients.get(ws);if(u){users.delete(u);clients.delete(ws);wss.clients.forEach(c=>send(c,{type:'presence',online:presence()}))}})
});
server.listen(PORT,()=>console.log(`ZIQVONA V1 running on port ${PORT}`));
