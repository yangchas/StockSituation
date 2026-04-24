
import subprocess
import psutil
import signal
import os
import logging
import asyncio
from datetime import datetime
from aiohttp import web

logger = logging.getLogger(__name__)

class T1ProcessManager:
    """exe进程管理器 - 单例模式"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(T1ProcessManager, cls).__new__(cls)
            cls._instance.t1_process = None
            cls._instance.t1_pid = None
            cls._instance.t1_exe_path = "/root/work/C/exe" # 保持原路径，或者应根据环境调整
        return cls._instance
    
    def __init__(self):
        pass

    def is_t1_running(self) -> dict:
        """检测exe是否正在运行"""
        try:
            # 检查是否有exe进程
            for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'create_time']):
                try:
                    if proc.info['name'] and 'exe' in proc.info['name'].lower():
                        return {
                            'running': True,
                            'pid': proc.info['pid'],
                            'name': proc.info['name'],
                            'start_time': datetime.fromtimestamp(proc.info['create_time']).strftime('%Y-%m-%d %H:%M:%S'),
                            'cmdline': proc.info['cmdline'] if proc.info['cmdline'] else []
                        }
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            
            return {'running': False, 'message': 'exe未运行'}
            
        except Exception as e:
            logger.error(f"❌ 检测T1进程状态失败: {e}")
            return {'running': False, 'error': str(e)}
    
    def start_t1(self, mode: str = 'live', replay_date: str = None, replay_time: str = None, replay_speed: float = 1.0) -> dict:
        """启动exe进程"""
        try:
            # 检查是否已经在运行
            status = self.is_t1_running()
            if status['running']:
                return {'success': False, 'message': 'exe已经在运行', 'pid': status['pid']}
            
            # 检查exe文件是否存在
            if not os.path.exists(self.t1_exe_path):
                # 尝试查找当前目录或上级目录
                if os.path.exists("./exe"):
                    self.t1_exe_path = "./exe"
                else: 
                     return {'success': False, 'message': f'exe文件不存在: {self.t1_exe_path}'}
            
            # 构建命令行参数
            cmd_args = [self.t1_exe_path]
            
            if mode == 'replay':
                cmd_args.append('--replay')
                if replay_date and replay_time:
                    # 组合日期和时间
                    start_datetime = f"{replay_date} {replay_time}"
                    cmd_args.extend(['--start', start_datetime])
                if replay_speed != 1.0:
                    cmd_args.extend(['--speed', str(replay_speed)])
                
                logger.info(f"🎯 启动回放模式: 日期={replay_date}, 时间={replay_time}, 速度={replay_speed}")
            else:
                cmd_args.append('--live')
                logger.info(f"🚀 启动实盘模式")
            
            # 启动进程（独立进程，避免Python关闭时被终止）
            if os.name == 'nt':  # Windows
                # 使用start命令启动独立进程
                cmd_line = 'start "T1 Process" /B ' + ' '.join(f'"{arg}"' for arg in cmd_args)
                os.system(cmd_line)
                
                # 等待进程启动，然后获取PID
                import time
                time.sleep(2)
                status = self.is_t1_running()
                if status['running']:
                    self.t1_pid = status['pid']
                    logger.info(f"🚀 exe已作为独立进程启动 (PID: {self.t1_pid})")
                else:
                    logger.warning("⚠️ 无法检测到exe进程，可能启动失败")
                    return {'success': False, 'message': 'exe启动失败'}
            else:  # Linux/Mac
                self.t1_process = subprocess.Popen(
                    cmd_args,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    start_new_session=True
                )
                self.t1_pid = self.t1_process.pid
            
            # self.t1_pid = self.t1_process.pid # redundant if windows logic sets it
            if self.t1_process:
                 self.t1_pid = self.t1_process.pid

            logger.info(f"🚀 exe已启动 (PID: {self.t1_pid}, 模式: {mode})")
            
            return {
                'success': True, 
                'pid': self.t1_pid, 
                'mode': mode,
                'message': f'exe启动成功 (PID: {self.t1_pid})'
            }
            
        except Exception as e:
            logger.error(f"❌ 启动exe失败: {e}")
            return {'success': False, 'error': str(e)}
    
    def stop_t1(self) -> dict:
        """停止exe进程"""
        try:
            # 只停止当前管理的进程，避免误杀其他exe进程
            if self.t1_pid:
                try:
                    process = psutil.Process(self.t1_pid)
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except psutil.TimeoutExpired:
                        process.kill()
                    
                    logger.info(f"⏹️ exe已停止 (PID: {self.t1_pid})")
                    self.t1_process = None
                    self.t1_pid = None
                    return {'success': True, 'message': f'exe已停止 (PID: {self.t1_pid})'}
                except psutil.NoSuchProcess:
                    # 如果进程已经不存在
                    logger.warning(f"⚠️ 无法找到exe进程 (PID: {self.t1_pid})")
                    self.t1_process = None
                    self.t1_pid = None
                    return {'success': False, 'message': 'exe进程不存在'}
            else:
                # 尝试查找并杀掉名为exe的进程（如果没有PID记录）
                for proc in psutil.process_iter(['pid', 'name']):
                    if proc.info['name'] and 'exe' in proc.info['name'].lower():
                         proc.terminate()
                         return {'success': True, 'message': f"强制停止发现的exe进程 (PID: {proc.info['pid']})"}
                
                return {'success': False, 'message': '没有正在管理的exe进程'}
                
        except Exception as e:
            logger.error(f"❌ 停止exe失败: {e}")
            return {'success': False, 'error': str(e)}

# 全局T1进程管理器实例
t1_manager = T1ProcessManager()

# T1进程状态API
async def t1_status_api(request):
    """获取exe运行状态"""
    try:
        status = t1_manager.is_t1_running()
        return web.json_response(status)
    except Exception as e:
        logger.error(f"❌ T1状态API错误: {e}")
        return web.json_response({'error': str(e)}, status=500)

# T1启动API
async def t1_start_api(request):
    """启动exe"""
    try:
        # 获取参数
        data = await request.json() if request.content_type == 'application/json' else {}
        mode = data.get('mode', 'live')
        replay_date = data.get('replay_date')
        replay_time = data.get('replay_time')
        replay_speed = float(data.get('replay_speed', 1.0))
        
        result = t1_manager.start_t1(mode, replay_date, replay_time, replay_speed)
        return web.json_response(result)
    except Exception as e:
        logger.error(f"❌ T1启动API错误: {e}")
        return web.json_response({'error': str(e)}, status=500)

# T1停止API
async def t1_stop_api(request):
    """停止exe"""
    try:
        result = t1_manager.stop_t1()
        return web.json_response(result)
    except Exception as e:
        logger.error(f"❌ T1停止API错误: {e}")
        return web.json_response({'error': str(e)}, status=500)
