
import requests
import time
from ..core.config import settings

# 이미지 수정
def bfl_image_edit(img_str: str, prompt: str):
  request = requests.post(
      'https://api.bfl.ai/v1/flux-kontext-pro',
      headers={
          'accept': 'application/json',
          'x-key': settings.bfl_api_key,
          'Content-Type': 'application/json',
      },
      json={
          'prompt': prompt,
          'input_image': img_str,
          'safety_tolerance': 6,
          'aspect_ratio': '3:4'
      },
  ).json()

  print(request)
  request_id = request["id"]
  polling_url = request["polling_url"]
  return {
      "request_id": request_id,
      "polling_url": polling_url,
  }

# 이미지 가져오기
def bfl_image_edit_polling(polling_url: str, request_id: str):
  while True:
    time.sleep(0.5)
    result = requests.get(
        polling_url,
        headers={
            'accept': 'application/json',
            'x-key': settings.bfl_api_key,
        },
        params={'id': request_id}
    ).json()
    
    if result['status'] == 'Ready':
        print(f"Image ready: {result['result']['sample']}")
        return result['result']['sample']
    elif result['status'] in ['Error', 'Failed']:
        print(f"Generation failed: {result}")
        return None
    else:
        return None