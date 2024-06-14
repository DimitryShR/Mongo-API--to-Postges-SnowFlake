class APIConnect:
    def __init__(self,
                 host: str,
                 file_name: str,
                 sort_field: str,
                 sort_direction: str,
                 limit: int
                 ) -> None:

        self.host = host
        self.file_name = file_name
        self.sort_field = sort_field
        self.sort_direction = sort_direction
        self.limit = limit

    def get_url(self) -> str:
        return '{host_}/{file_name_}?sort_field={sort_field_}&sort_direction={sort_direction_}&limit={limit_}'.format(
            host_=self.host,
            file_name_ = self.file_name,
            sort_field_ = self.sort_field,
            sort_direction_ = self.sort_direction,
            limit_ = self.limit)

class APIHeaders:
    def __init__(self,
                 nickname: str,
                 cohort: int,
                 api_key: str
                ) -> None:
       
       self.nickname = nickname
       self.cohort = cohort
       self.api_key = api_key
    
    def get_headers(self) -> dict:
        return {'X-Nickname': self.nickname, 
                'X-Cohort': self.cohort,
                'X-API-KEY': self.api_key
                 }
