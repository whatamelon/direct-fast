"""
Google Sheets API 인터페이스

이 모듈은 Google Sheets API를 사용하여 스프레드시트의 데이터를 읽고 쓰며,
다양한 업데이트 작업을 수행하는 기능을 제공합니다.

주요 기능:
- 셀 값 읽기/쓰기 (get, batchGet, update, batchUpdate, append)
- 스프레드시트 업데이트 (시트 추가/삭제/복제, 속성 업데이트)
- 에러 처리 및 유틸리티 함수
"""

import os
import json
from typing import List, Dict, Any, Optional, Union, Tuple
from datetime import datetime
import logging

from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from ..core.config import get_settings

# 로깅 설정
logger = logging.getLogger(__name__)

# Google Sheets API 스코프
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

class GoogleSheetError(Exception):
    """Google Sheets API 관련 에러를 위한 커스텀 예외 클래스"""
    pass

class GoogleSheetInterface:
    """Google Sheets API를 사용한 스프레드시트 조작 인터페이스"""
    
    def __init__(self, credentials_path: Optional[str] = None, token_path: Optional[str] = None):
        """
        Google Sheets API 인터페이스 초기화
        
        Args:
            credentials_path: OAuth2 클라이언트 자격 증명 파일 경로
            token_path: 토큰 저장 파일 경로
        """
        self.settings = get_settings()
        self.credentials_path = "./src/assets/dept-meta-advertise-a17cb36a4928.json"
        self.token_path = "./src/assets/token.json"
        self.service = None
        self._authenticate()
    
    def _authenticate(self):
        """Google Sheets API 인증 수행"""
        try:
            creds = None
            # 유효한 자격 증명이 없는 경우 새로 인증
            if not creds or not creds.valid:
              # 서비스 계정 키 파일이 있는 경우 사용
              service_account_path = "./src/assets/dept-meta-advertise-a17cb36a4928.json"
              if service_account_path and os.path.exists(service_account_path):
                  creds = ServiceAccountCredentials.from_service_account_file(
                      service_account_path, scopes=SCOPES
                  )
            # Google Sheets API 서비스 빌드
            self.service = build('sheets', 'v4', credentials=creds)
            logger.info("Google Sheets API 인증 성공")
            
        except Exception as e:
            logger.error(f"Google Sheets API 인증 실패: {str(e)}")
            raise GoogleSheetError(f"인증 실패: {str(e)}")
    
    # ==================== 셀 값 읽기 기능 ====================
    
    def read_values(self, spreadsheet_id: str, range_name: str) -> List[List[str]]:
        """
        지정된 범위의 셀 값을 읽습니다.
        
        Args:
            spreadsheet_id: 스프레드시트 ID
            range_name: 읽을 범위 (예: 'Sheet1!A1:C10')
            
        Returns:
            셀 값들의 2차원 리스트
        """
        try:
            result = self.service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            logger.info(f"범위 {range_name}에서 {len(values)}행 읽기 완료")
            return values
            
        except HttpError as e:
            logger.error(f"값 읽기 실패: {str(e)}")
            raise GoogleSheetError(f"값 읽기 실패: {str(e)}")
    
    def read_multiple_ranges(self, spreadsheet_id: str, ranges: List[str]) -> Dict[str, List[List[str]]]:
        """
        여러 범위의 셀 값을 한 번에 읽습니다.
        
        Args:
            spreadsheet_id: 스프레드시트 ID
            ranges: 읽을 범위들의 리스트
            
        Returns:
            범위별 셀 값들의 딕셔너리
        """
        try:
            result = self.service.spreadsheets().values().batchGet(
                spreadsheetId=spreadsheet_id,
                ranges=ranges
            ).execute()
            
            value_ranges = result.get('valueRanges', [])
            result_dict = {}
            
            for i, value_range in enumerate(value_ranges):
                range_name = ranges[i]
                values = value_range.get('values', [])
                result_dict[range_name] = values
            
            logger.info(f"{len(ranges)}개 범위 읽기 완료")
            return result_dict
            
        except HttpError as e:
            logger.error(f"다중 범위 읽기 실패: {str(e)}")
            raise GoogleSheetError(f"다중 범위 읽기 실패: {str(e)}")
    
    # ==================== 셀 값 쓰기 기능 ====================
    
    def write_values(self, spreadsheet_id: str, range_name: str, values: List[List[str]], 
                    value_input_option: str = 'RAW') -> Dict[str, Any]:
        """
        지정된 범위에 셀 값을 씁니다.
        
        Args:
            spreadsheet_id: 스프레드시트 ID
            range_name: 쓸 범위 (예: 'Sheet1!A1:C10')
            values: 쓸 값들의 2차원 리스트
            value_input_option: 값 입력 옵션 ('RAW' 또는 'USER_ENTERED')
            
        Returns:
            업데이트 결과 정보
        """
        try:
            body = {
                'values': values
            }
            
            result = self.service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption=value_input_option,
                body=body
            ).execute()
            
            logger.info(f"범위 {range_name}에 {len(values)}행 쓰기 완료")
            return result
            
        except HttpError as e:
            logger.error(f"값 쓰기 실패: {str(e)}")
            raise GoogleSheetError(f"값 쓰기 실패: {str(e)}")
    
    def write_multiple_ranges(self, spreadsheet_id: str, 
                             data: List[Dict[str, Any]], 
                             value_input_option: str = 'RAW') -> Dict[str, Any]:
        """
        여러 범위에 셀 값을 한 번에 씁니다.
        
        Args:
            spreadsheet_id: 스프레드시트 ID
            data: 범위별 데이터 리스트 [{'range': 'Sheet1!A1:C10', 'values': [['A', 'B', 'C']]}]
            value_input_option: 값 입력 옵션 ('RAW' 또는 'USER_ENTERED')
            
        Returns:
            배치 업데이트 결과 정보
        """
        try:
            body = {
                'valueInputOption': value_input_option,
                'data': data
            }
            
            result = self.service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()
            
            logger.info(f"{len(data)}개 범위 쓰기 완료")
            return result
            
        except HttpError as e:
            logger.error(f"다중 범위 쓰기 실패: {str(e)}")
            raise GoogleSheetError(f"다중 범위 쓰기 실패: {str(e)}")
    
    def append_values(self, spreadsheet_id: str, range_name: str, values: List[List[str]], 
                     value_input_option: str = 'RAW') -> Dict[str, Any]:
        """
        기존 데이터에 새로운 값을 추가합니다.
        
        Args:
            spreadsheet_id: 스프레드시트 ID
            range_name: 추가할 범위 (예: 'Sheet1!A:C')
            values: 추가할 값들의 2차원 리스트
            value_input_option: 값 입력 옵션 ('RAW' 또는 'USER_ENTERED')
            
        Returns:
            추가 결과 정보
        """
        try:
            body = {
                'values': values
            }
            
            result = self.service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption=value_input_option,
                body=body
            ).execute()
            
            logger.info(f"범위 {range_name}에 {len(values)}행 추가 완료")
            return result
            
        except HttpError as e:
            logger.error(f"값 추가 실패: {str(e)}")
            raise GoogleSheetError(f"값 추가 실패: {str(e)}")
    
    # ==================== 스프레드시트 업데이트 기능 ====================
    
    def add_sheet(self, spreadsheet_id: str, sheet_name: str, 
                  rows: int = 1000, cols: int = 26) -> Dict[str, Any]:
        """
        새로운 시트를 추가합니다.
        
        Args:
            spreadsheet_id: 스프레드시트 ID
            sheet_name: 새 시트 이름
            rows: 행 수
            cols: 열 수
            
        Returns:
            시트 추가 결과 정보
        """
        try:
            requests = [{
                'addSheet': {
                    'properties': {
                        'title': sheet_name,
                        'gridProperties': {
                            'rowCount': rows,
                            'columnCount': cols
                        }
                    }
                }
            }]
            
            body = {'requests': requests}
            
            result = self.service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()
            
            logger.info(f"시트 '{sheet_name}' 추가 완료")
            return result
            
        except HttpError as e:
            logger.error(f"시트 추가 실패: {str(e)}")
            raise GoogleSheetError(f"시트 추가 실패: {str(e)}")
    
    def delete_sheet(self, spreadsheet_id: str, sheet_id: int) -> Dict[str, Any]:
        """
        시트를 삭제합니다.
        
        Args:
            spreadsheet_id: 스프레드시트 ID
            sheet_id: 삭제할 시트 ID
            
        Returns:
            시트 삭제 결과 정보
        """
        try:
            requests = [{
                'deleteSheet': {
                    'sheetId': sheet_id
                }
            }]
            
            body = {'requests': requests}
            
            result = self.service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()
            
            logger.info(f"시트 ID {sheet_id} 삭제 완료")
            return result
            
        except HttpError as e:
            logger.error(f"시트 삭제 실패: {str(e)}")
            raise GoogleSheetError(f"시트 삭제 실패: {str(e)}")
    
    def duplicate_sheet(self, spreadsheet_id: str, sheet_id: int, 
                       new_sheet_name: str) -> Dict[str, Any]:
        """
        기존 시트를 복제합니다.
        
        Args:
            spreadsheet_id: 스프레드시트 ID
            sheet_id: 복제할 시트 ID
            new_sheet_name: 새 시트 이름
            
        Returns:
            시트 복제 결과 정보
        """
        try:
            requests = [{
                'duplicateSheet': {
                    'sourceSheetId': sheet_id,
                    'newSheetName': new_sheet_name
                }
            }]
            
            body = {'requests': requests}
            
            result = self.service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()
            
            logger.info(f"시트 ID {sheet_id}를 '{new_sheet_name}'으로 복제 완료")
            return result
            
        except HttpError as e:
            logger.error(f"시트 복제 실패: {str(e)}")
            raise GoogleSheetError(f"시트 복제 실패: {str(e)}")
    
    def update_sheet_properties(self, spreadsheet_id: str, sheet_id: int, 
                               properties: Dict[str, Any]) -> Dict[str, Any]:
        """
        시트의 속성을 업데이트합니다.
        
        Args:
            spreadsheet_id: 스프레드시트 ID
            sheet_id: 업데이트할 시트 ID
            properties: 업데이트할 속성들
            
        Returns:
            속성 업데이트 결과 정보
        """
        try:
            requests = [{
                'updateSheetProperties': {
                    'properties': {
                        'sheetId': sheet_id,
                        **properties
                    },
                    'fields': ','.join(properties.keys())
                }
            }]
            
            body = {'requests': requests}
            
            result = self.service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()
            
            logger.info(f"시트 ID {sheet_id} 속성 업데이트 완료")
            return result
            
        except HttpError as e:
            logger.error(f"시트 속성 업데이트 실패: {str(e)}")
            raise GoogleSheetError(f"시트 속성 업데이트 실패: {str(e)}")
    
    def update_dimension_properties(self, spreadsheet_id: str, sheet_id: int, 
                                   dimension: str, start_index: int, end_index: int,
                                   properties: Dict[str, Any]) -> Dict[str, Any]:
        """
        행 또는 열의 속성을 업데이트합니다.
        
        Args:
            spreadsheet_id: 스프레드시트 ID
            sheet_id: 시트 ID
            dimension: 차원 ('ROWS' 또는 'COLUMNS')
            start_index: 시작 인덱스
            end_index: 끝 인덱스
            properties: 업데이트할 속성들
            
        Returns:
            차원 속성 업데이트 결과 정보
        """
        try:
            requests = [{
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': dimension,
                        'startIndex': start_index,
                        'endIndex': end_index
                    },
                    'properties': properties,
                    'fields': ','.join(properties.keys())
                }
            }]
            
            body = {'requests': requests}
            
            result = self.service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()
            
            logger.info(f"시트 ID {sheet_id} {dimension} 속성 업데이트 완료")
            return result
            
        except HttpError as e:
            logger.error(f"차원 속성 업데이트 실패: {str(e)}")
            raise GoogleSheetError(f"차원 속성 업데이트 실패: {str(e)}")
    
    # ==================== 유틸리티 함수 ====================
    
    def get_sheet_info(self, spreadsheet_id: str) -> Dict[str, Any]:
        """
        스프레드시트의 기본 정보를 가져옵니다.
        
        Args:
            spreadsheet_id: 스프레드시트 ID
            
        Returns:
            스프레드시트 정보
        """
        try:
            result = self.service.spreadsheets().get(
                spreadsheetId=spreadsheet_id
            ).execute()
            
            logger.info(f"스프레드시트 정보 조회 완료: {result.get('properties', {}).get('title', 'Unknown')}")
            return result
            
        except HttpError as e:
            logger.error(f"스프레드시트 정보 조회 실패: {str(e)}")
            raise GoogleSheetError(f"스프레드시트 정보 조회 실패: {str(e)}")
    
    def list_sheets(self, spreadsheet_id: str) -> List[Dict[str, Any]]:
        """
        스프레드시트의 모든 시트 정보를 가져옵니다.
        
        Args:
            spreadsheet_id: 스프레드시트 ID
            
        Returns:
            시트 정보 리스트
        """
        try:
            result = self.get_sheet_info(spreadsheet_id)
            sheets = result.get('sheets', [])
            
            sheet_info = []
            for sheet in sheets:
                properties = sheet.get('properties', {})
                sheet_info.append({
                    'sheetId': properties.get('sheetId'),
                    'title': properties.get('title'),
                    'index': properties.get('index'),
                    'sheetType': properties.get('sheetType'),
                    'gridProperties': properties.get('gridProperties', {})
                })
            
            logger.info(f"{len(sheet_info)}개 시트 정보 조회 완료")
            return sheet_info
            
        except Exception as e:
            logger.error(f"시트 목록 조회 실패: {str(e)}")
            raise GoogleSheetError(f"시트 목록 조회 실패: {str(e)}")
    
    def clear_range(self, spreadsheet_id: str, range_name: str) -> Dict[str, Any]:
        """
        지정된 범위의 셀을 지웁니다.
        
        Args:
            spreadsheet_id: 스프레드시트 ID
            range_name: 지울 범위
            
        Returns:
            지우기 결과 정보
        """
        try:
            result = self.service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            
            logger.info(f"범위 {range_name} 지우기 완료")
            return result
            
        except HttpError as e:
            logger.error(f"범위 지우기 실패: {str(e)}")
            raise GoogleSheetError(f"범위 지우기 실패: {str(e)}")
    
    def format_range(self, spreadsheet_id: str, range_name: str, 
                    format_requests: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        지정된 범위의 셀 서식을 적용합니다.
        
        Args:
            spreadsheet_id: 스프레드시트 ID
            range_name: 서식을 적용할 범위
            format_requests: 서식 요청 리스트
            
        Returns:
            서식 적용 결과 정보
        """
        try:
            requests = []
            for format_req in format_requests:
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': 0,  # 첫 번째 시트 (시트 ID로 변경 가능)
                            'startRowIndex': format_req.get('startRowIndex', 0),
                            'endRowIndex': format_req.get('endRowIndex', 1),
                            'startColumnIndex': format_req.get('startColumnIndex', 0),
                            'endColumnIndex': format_req.get('endColumnIndex', 1)
                        },
                        'cell': {
                            'userEnteredFormat': format_req.get('format', {})
                        },
                        'fields': 'userEnteredFormat'
                    }
                })
            
            body = {'requests': requests}
            
            result = self.service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()
            
            logger.info(f"범위 {range_name} 서식 적용 완료")
            return result
            
        except HttpError as e:
            logger.error(f"서식 적용 실패: {str(e)}")
            raise GoogleSheetError(f"서식 적용 실패: {str(e)}")


# 편의 함수들
def create_google_sheet_interface(credentials_path: Optional[str] = None, 
                                 token_path: Optional[str] = None) -> GoogleSheetInterface:
    """
    Google Sheets 인터페이스 인스턴스를 생성합니다.
    
    Args:
        credentials_path: OAuth2 클라이언트 자격 증명 파일 경로
        token_path: 토큰 저장 파일 경로
        
    Returns:
        GoogleSheetInterface 인스턴스
    """
    return GoogleSheetInterface(credentials_path, token_path)


def get_spreadsheet_id_from_url(url: str) -> str:
    """
    Google Sheets URL에서 스프레드시트 ID를 추출합니다.
    
    Args:
        url: Google Sheets URL
        
    Returns:
        스프레드시트 ID
    """
    import re
    
    # URL 패턴 매칭
    pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
    match = re.search(pattern, url)
    
    if match:
        return match.group(1)
    else:
        raise ValueError("유효하지 않은 Google Sheets URL입니다.")


# 사용 예시
if __name__ == "__main__":
    # 인터페이스 생성
    gs = create_google_sheet_interface()
    
    # 스프레드시트 ID (예시)
    spreadsheet_id = "your-spreadsheet-id-here"
    
    try:
        # 시트 정보 조회
        sheets = gs.list_sheets(spreadsheet_id)
        print(f"시트 목록: {[sheet['title'] for sheet in sheets]}")
        
        # 데이터 읽기
        values = gs.read_values(spreadsheet_id, "Sheet1!A1:C10")
        print(f"읽은 데이터: {values}")
        
        # 데이터 쓰기
        new_data = [["새로운", "데이터", "행"]]
        result = gs.append_values(spreadsheet_id, "Sheet1!A:C", new_data)
        print(f"데이터 추가 결과: {result}")
        
    except GoogleSheetError as e:
        print(f"에러 발생: {e}")
