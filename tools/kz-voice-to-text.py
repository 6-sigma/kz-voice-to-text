from collections.abc import Generator
from typing import Any
import os
import json, sys, time, logging
import datetime
import oss2
import requests
from oss2.credentials import EnvironmentVariableCredentialsProvider
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.request import CommonRequest
from aliyunsdkcore.auth.credentials import AccessKeyCredential

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

credentials = None
client = None

def create_common_request(domain, version, protocolType, method, uri):
    request = CommonRequest()
    request.set_accept_format('json')
    request.set_domain(domain)
    request.set_version(version)
    request.set_protocol_type(protocolType)
    request.set_method(method)
    request.set_uri_pattern(uri)
    request.add_header('Content-Type', 'application/json')
    return request

class KzVoiceToTextTool(Tool):
    def _upload_voice_file(self, voice_file_url: str, file_name: str) -> str:
        """上传语音文件到OSS并返回URL"""
        try:
            logging.info(f"voice_file_url: {voice_file_url}")
            # 初始化OSS客户端
            auth = oss2.ProviderAuthV4(EnvironmentVariableCredentialsProvider())
            endpoint = "https://oss-cn-beijing.aliyuncs.com"
            bucket_name = "kzbucket"
            bucket = oss2.Bucket(auth, endpoint, bucket_name, region="cn-beijing")

            # 上传文件
            # file_name = "voice_file"
            file_content = requests.get(voice_file_url).content
            
            put_result = bucket.put_object(
                file_name,
                file_content,
                headers={'Content-Type': 'audio/mpeg'}
            )

            result_dict = {
                'status': put_result.status,
                'request_id': put_result.request_id,
                'etag': put_result.etag,
                'version_id': getattr(put_result, 'version_id', None),
                'crc64': getattr(put_result, 'crc64', None)
            }
            print(result_dict)

            return f"https://{bucket_name}.{endpoint.replace('https://', '')}/{file_name}"
        except Exception as e:
            sys.excepthook(*sys.exc_info())
            logging.error(f"上传失败: {str(e)}")
            raise

    def _init_parameters(self, file_url: str, app_key: str) -> dict:
        return {
            'AppKey': app_key or 'RPUYI4dbUfFsbDTm',
            'Input': {
                'SourceLanguage': 'cn',
                'TaskKey': 'task' + datetime.datetime.now().strftime('%Y%m%d%H%M%S'),
                'FileUrl': file_url,
                'Title': '语音转文本任务',
                'ContentType': 'audio'
            },
            'Parameters': {
                'Transcription': {
                    'DiarizationEnabled': True,
                    'Diarization': {
                        'SpeakerCount': 0
                    },
                    'OutputFormat': 'txt'
                }
            }
        }

    def format_transcription_to_string(self, json_data):
        # 检查输入是否是列表并取第一个元素
        if isinstance(json_data, list) and len(json_data) > 0:
            json_data = json_data[0]
        
        # 获取 Transcription 部分
        transcription = json_data.get("Transcription", {})
        paragraphs = transcription.get("Paragraphs", [])
        
        # 存储格式化后的结果
        formatted_text = []
        
        # 遍历每个段落
        for paragraph in paragraphs:
            speaker_id = paragraph.get("SpeakerId", "Unknown")
            words = paragraph.get("Words", [])
            
            # 将该段落的所有单词连接成一句话
            sentence = "".join(word.get("Text", "") for word in words)
            
            # 格式化为 "speakerX: 内容" 的形式
            formatted_line = f"speaker{speaker_id}: {sentence}"
            formatted_text.append(formatted_line)
        
        # 将所有行用换行符连接
        return "\n".join(formatted_text)

    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        access_id = self.runtime.credentials.get("access_id", "")
        if not access_id:
            raise ValueError("invalid ali cloud access_id")
        access_secret = self.runtime.credentials.get("access_secret", "")
        if not access_secret:
            raise ValueError("invalid ali cloud access_secret")

        os.environ.setdefault('OSS_ACCESS_KEY_ID', access_id)
        os.environ.setdefault('OSS_ACCESS_KEY_SECRET', access_secret)

        global credentials
        credentials = AccessKeyCredential(
            access_id or "",
            access_secret or ""
        )
        global client
        client = AcsClient(region_id='cn-beijing', credential=credentials)

        app_key = self.runtime.credentials.get("app_key", "")

        voice_file_url = tool_parameters.get('voice_file')
        if not voice_file_url:
            yield self.create_text_message("缺少语音文件参数")
            return

        try:
            # 上传文件到OSS
            logging.info(f"开始上传文件: {voice_file_url}")
            oss_url = self._upload_voice_file(voice_file_url.url, voice_file_url.filename)
            logging.info(f"文件已上传至: {oss_url}")

            file_name = voice_file_url.filename

            # 调用语音转文本服务
            # body = self._init_parameters(oss_url)
            print(oss_url)
            body = self._init_parameters(f"https://kzbucket.oss-cn-beijing.aliyuncs.com/{file_name}", app_key)
            request = create_common_request(
                'tingwu.cn-beijing.aliyuncs.com',
                '2023-09-30',
                'https',
                'PUT',
                '/openapi/tingwu/v2/tasks'
            )
            request.add_query_param('type', 'offline')
            request.set_content(json.dumps(body).encode('utf-8'))
            request.add_header('x-acs-tingwu-version', '2023-09-30')

            response = client.do_action_with_exception(request)
            result = json.loads(response)
            logging.info(f"任务创建成功: {result}")

            # 轮询任务结果
            task_id = result['Data']['TaskId']
            while True:
                request = create_common_request(
                    'tingwu.cn-beijing.aliyuncs.com',
                    '2023-09-30',
                    'https',
                    'GET',
                    f'/openapi/tingwu/v2/tasks/{task_id}'
                )
                response = client.do_action_with_exception(request)
                task_result = json.loads(response)
                logging.info(f"任务状态: {task_result}")
                
                if task_result['Data']['TaskStatus'] == 'COMPLETED':
                    _voice_to_text_result = requests.get(task_result['Data']['Result']['Transcription']).json()
                    logging.info(f"任务处理成功: {_voice_to_text_result}")

                    strs = self.format_transcription_to_string(_voice_to_text_result)
                    # yield self.create_json_message({
                    #     "status": "success",
                    #     "result": _voice_to_text_result
                    # })
                    yield self.create_text_message(strs)
                    break
                elif task_result['Data']['TaskStatus'] == 'FAILED':
                    yield self.create_text_message(f"任务处理失败: {task_result['Data'].get('ErrorMessage', '未知错误')}")
                    break

                time.sleep(1)

        except Exception as e:
            sys.excepthook(*sys.exc_info())
            logging.error(f"处理失败: {str(e)}")
            yield self.create_text_message(f"处理失败: {str(e)}")
