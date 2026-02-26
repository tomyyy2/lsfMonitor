"""
Internationalization (i18n) support for lsfMonitor
"""

import os
import json
from typing import Dict, Any

# Define supported languages
SUPPORTED_LANGUAGES = {
    'en': 'English',
    'zh': '中文'
}

class I18nManager:
    """
    Internationalization manager class
    """
    def __init__(self, lang_code: str = 'en'):
        """
        Initialize the i18n manager with the specified language
        """
        self.lang_code = lang_code
        self.translations = {}
        self.load_translations()
    
    def load_translations(self):
        """
        Load translations for the specified language
        """
        # Define translation resources
        en_translations = {
            # Menu items
            'File': 'File',
            'Setup': 'Setup',
            'Function': 'Function',
            'Help': 'Help',
            
            # Menu actions
            'Export jobs table': 'Export jobs table',
            'Export hosts table': 'Export hosts table',
            'Export users table': 'Export users table',
            'Export queues table': 'Export queues table',
            'Export utilization table': 'Export utilization table',
            'Export license feature table': 'Export license feature table',
            'Export license expires table': 'Export license expires table',
            'Exit': 'Exit',
            
            # Setup menu
            'Enable queue detail': 'Enable queue detail',
            'Enable utilization detail': 'Enable utilization detail',
            
            # Function menu
            'Check Pend reason': 'Check Pend reason',
            'Check Slow reason': 'Check Slow reason',
            'Check Fail reason': 'Check Fail reason',
            
            # Help menu
            'Version': 'Version',
            'About lsfMonitor': 'About lsfMonitor',
            
            # Tab names
            'JOB': 'JOB',
            'JOBS': 'JOBS',
            'HOSTS': 'HOSTS',
            'LOAD': 'LOAD',
            'USERS': 'USERS',
            'QUEUES': 'QUEUES',
            'UTILIZATION': 'UTILIZATION',
            'LICENSE': 'LICENSE',
            
            # Job tab elements
            'Job': 'Job',
            'Check': 'Check',
            'Kill': 'Kill',
            'Trace': 'Trace',
            'Status': 'Status',
            'User': 'User',
            'Project': 'Project',
            'Queue': 'Queue',
            'Host': 'Host',
            'Start Time': 'Start Time',
            'Finish Time': 'Finish Time',
            'Processors': 'Processors',
            'Rusage': 'Rusage',
            'Mem (now)': 'Mem (now)',
            'Mem (max)': 'Mem (max)',
            
            # Jobs tab elements
            'Job': 'Job',
            'User': 'User',
            'Status': 'Status',
            'Queue': 'Queue',
            'Host': 'Host',
            'Started': 'Started',
            'Project': 'Project',
            'Slot': 'Slot',
            'Rusage (G)': 'Rusage (G)',
            'Mem (G)': 'Mem (G)',
            'Command': 'Command',
            
            # Hosts tab elements
            'Host': 'Host',
            'Status': 'Status',
            'Queue': 'Queue',
            'MAX': 'MAX',
            'Njobs': 'Njobs',
            'Ut (%)': 'Ut (%)',
            'MaxMem (G)': 'MaxMem (G)',
            'aMem (G)': 'aMem (G)',
            'saMem (G)': 'saMem (G)',
            'MaxSwp (G)': 'MaxSwp (G)',
            'Swp (G)': 'Swp (G)',
            'Tmp (G)': 'Tmp (G)',
            
            # Users tab elements
            'User': 'User',
            'Job_Num': 'Job Num',
            'Pass_Rate (%)': 'Pass Rate (%)',
            'Total_Rusage_Mem (G)': 'Total Rusage Mem (G)',
            'Avg_Rusage_Mem (G)': 'Avg Rusage Mem (G)',
            'Total_Max_Mem (G)': 'Total Max Mem (G)',
            'Avg_Max_Mem (G)': 'Avg Max Mem (G)',
            'Total_Mem_Waste (G)': 'Total Mem Waste (G)',
            'Avg_Mem_Waste (G)': 'Avg Mem Waste (G)',
            
            # Queues tab elements
            'QUEUE': 'QUEUE',
            'SLOTS': 'SLOTS',
            'PEND': 'PEND',
            'RUN': 'RUN',
            
            # Utilization tab elements
            'Queue': 'Queue',
            'slots': 'Slots',
            'slot(%)': 'Slot(%)',
            'cpu(%)': 'CPU(%)',
            'mem(%)': 'MEM(%)',
            
            # License tab elements
            'Server': 'Server',
            'Vendor': 'Vendor',
            'Feature': 'Feature',
            'Issued': 'Issued',
            'In_Use': 'In Use',
            'License Server': 'License Server',
            'Num': 'Num',
            'Expires': 'Expires',
            'Show': 'Show',
            
            # Common messages
            'Loading LSF {lsf_info} information ...': 'Loading LSF {lsf_info} information ...',
            'Loading License information ...': 'Loading License information ...',
            'Kill {jobid} successfully!': 'Kill {jobid} successfully!',
            'Kill {jobid} fail': 'Kill {jobid} fail',
            'Loading LSF jobs information ...': 'Loading LSF jobs information ...',
            'Loading ut/mem load information ...': 'Loading ut/mem load information ...',
            'Loading user history info ...': 'Loading user history info ...',
            'Loading queue utilization info ...': 'Loading queue utilization info ...',
            'Loading resource utilization information ...': 'Loading resource utilization information ...',
            'Drawing {selected_resource} curve ...': 'Drawing {selected_resource} curve ...',
            
            # About dialog
            'Thanks for downloading lsfMonitor.\n\nlsfMonitor is an open source software for LSF information data-collection, data-analysis and data-display.\n\nPlease contact with liyanqing1987@163.com with any question.': 'Thanks for downloading lsfMonitor.\n\nlsfMonitor is an open source software for LSF information data-collection, data-analysis and data-display.\n\nPlease contact with liyanqing1987@163.com with any question.',
            
            # Version dialog
            'Version: {version} ({date})': 'Version: {version} ({date})',
        }
        
        zh_translations = {
            # Menu items
            'File': '文件',
            'Setup': '设置',
            'Function': '功能',
            'Help': '帮助',
            
            # Menu actions
            'Export jobs table': '导出作业表',
            'Export hosts table': '导出主机表',
            'Export users table': '导出用户表',
            'Export queues table': '导出队列表',
            'Export utilization table': '导出利用率表',
            'Export license feature table': '导出许可证特性表',
            'Export license expires table': '导出许可证过期表',
            'Exit': '退出',
            
            # Setup menu
            'Enable queue detail': '启用队列详情',
            'Enable utilization detail': '启用利用率详情',
            
            # Function menu
            'Check Pend reason': '检查等待原因',
            'Check Slow reason': '检查缓慢原因',
            'Check Fail reason': '检查失败原因',
            
            # Help menu
            'Version': '版本',
            'About lsfMonitor': '关于 lsfMonitor',
            
            # Tab names
            'JOB': '作业',
            'JOBS': '作业列表',
            'HOSTS': '主机',
            'LOAD': '负载',
            'USERS': '用户',
            'QUEUES': '队列',
            'UTILIZATION': '利用率',
            'LICENSE': '许可证',
            
            # Job tab elements
            'Job': '作业',
            'Check': '检查',
            'Kill': '终止',
            'Trace': '追踪',
            'Status': '状态',
            'User': '用户',
            'Project': '项目',
            'Queue': '队列',
            'Host': '主机',
            'Start Time': '开始时间',
            'Finish Time': '结束时间',
            'Processors': '处理器',
            'Rusage': '资源使用',
            'Mem (now)': '内存(当前)',
            'Mem (max)': '内存(最大)',
            
            # Jobs tab elements
            'Job': '作业',
            'User': '用户',
            'Status': '状态',
            'Queue': '队列',
            'Host': '主机',
            'Started': '已启动',
            'Project': '项目',
            'Slot': '槽位',
            'Rusage (G)': '资源使用(G)',
            'Mem (G)': '内存(G)',
            'Command': '命令',
            
            # Hosts tab elements
            'Host': '主机',
            'Status': '状态',
            'Queue': '队列',
            'MAX': '最大',
            'Njobs': '作业数',
            'Ut (%)': '利用率(%)',
            'MaxMem (G)': '最大内存(G)',
            'aMem (G)': '可用内存(G)',
            'saMem (G)': '共享可用内存(G)',
            'MaxSwp (G)': '最大交换(G)',
            'Swp (G)': '交换(G)',
            'Tmp (G)': '临时(G)',
            
            # Users tab elements
            'User': '用户',
            'Job_Num': '作业数',
            'Pass_Rate (%)': '通过率(%)',
            'Total_Rusage_Mem (G)': '总资源使用内存(G)',
            'Avg_Rusage_Mem (G)': '平均资源使用内存(G)',
            'Total_Max_Mem (G)': '总最大内存(G)',
            'Avg_Max_Mem (G)': '平均最大内存(G)',
            'Total_Mem_Waste (G)': '总内存浪费(G)',
            'Avg_Mem_Waste (G)': '平均内存浪费(G)',
            
            # Queues tab elements
            'QUEUE': '队列',
            'SLOTS': '槽位',
            'PEND': '等待',
            'RUN': '运行',
            
            # Utilization tab elements
            'Queue': '队列',
            'slots': '槽位',
            'slot(%)': '槽位(%)',
            'cpu(%)': 'CPU(%)',
            'mem(%)': '内存(%)',
            
            # License tab elements
            'Server': '服务器',
            'Vendor': '供应商',
            'Feature': '特性',
            'Issued': '已发布',
            'In_Use': '正在使用',
            'License Server': '许可证服务器',
            'Num': '数量',
            'Expires': '过期',
            'Show': '显示',
            
            # Common messages
            'Loading LSF {lsf_info} information ...': '正在加载LSF {lsf_info} 信息...',
            'Loading License information ...': '正在加载许可证信息...',
            'Kill {jobid} successfully!': '成功终止 {jobid}!',
            'Kill {jobid} fail': '终止 {jobid} 失败',
            'Loading LSF jobs information ...': '正在加载LSF作业信息...',
            'Loading ut/mem load information ...': '正在加载利用率/内存负载信息...',
            'Loading user history info ...': '正在加载用户历史信息...',
            'Loading queue utilization info ...': '正在加载队列利用率信息...',
            'Loading resource utilization information ...': '正在加载资源利用率信息...',
            'Drawing {selected_resource} curve ...': '正在绘制 {selected_resource} 曲线...',
            
            # About dialog
            'Thanks for downloading lsfMonitor.\n\nlsfMonitor is an open source software for LSF information data-collection, data-analysis and data-display.\n\nPlease contact with liyanqing1987@163.com with any question.': '感谢下载lsfMonitor。\n\nlsfMonitor是一个用于LSF信息数据收集、分析和显示的开源软件。\n\n如有任何问题，请联系 liyanqing1987@163.com。',
            
            # Version dialog
            'Version: {version} ({date})': '版本: {version} ({date})',
        }
        
        # Select the appropriate translation dictionary
        if self.lang_code == 'zh':
            self.translations = zh_translations
        else:
            self.translations = en_translations
    
    def translate(self, text: str, **kwargs) -> str:
        """
        Translate the given text to the current language
        """
        translated_text = self.translations.get(text, text)
        if kwargs:
            try:
                translated_text = translated_text.format(**kwargs)
            except KeyError:
                # If formatting fails, return the original translated text
                pass
        return translated_text
    
    def set_language(self, lang_code: str):
        """
        Change the current language
        """
        if lang_code in SUPPORTED_LANGUAGES:
            self.lang_code = lang_code
            self.load_translations()

# Global i18n instance
_i18n_manager = I18nManager()

def set_language(lang_code: str):
    """
    Set the application language
    """
    global _i18n_manager
    _i18n_manager.set_language(lang_code)

def t(text: str, **kwargs) -> str:
    """
    Translate the given text
    """
    return _i18n_manager.translate(text, **kwargs)