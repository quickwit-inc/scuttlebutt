Get-Process "chitchat-test" | Stop-Process

cargo build --release

for ($i = 10000; $i -lt 10100; $i++)
{
    $listen_addr = "127.0.0.1:$i";
    Write-Host $listen_addr;

    Start-Process -NoNewWindow "cargo" -ArgumentList "run --release -- --listen_addr $listen_addr --seed 127.0.0.1:10002 --node_id node_$i"
}

Read-Host
