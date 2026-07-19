import psutil

def kill_process_on_port(*ports):
    for proc in psutil.process_iter(['pid', 'name', 'net_connections']):
        if proc.info['net_connections']:
            for conn in proc.info['net_connections']:
                if conn.laddr.port in ports:
                    print(f"Killing process {proc.info['name']} (PID: {proc.info['pid']})")
                    psutil.Process(proc.info['pid']).terminate()

