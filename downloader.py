import aiohttp
import glob
import asyncio
import os
import logging

import aiofiles

from main import create_cookies ,create_pandas_dataframe_and_delete_file_T2,create_pandas_dataframe_and_delete_file_O3,create_pandas_dataframe_and_delete_file_Rainfall,create_pandas_dataframe_and_delete_file_Rainfall2,create_pandas_dataframe_and_delete_file_PM25,create_pandas_dataframe_and_delete_file_PM25_second_way,create_pandas_dataframe_and_delete_file_TO3,create_pandas_dataframe_and_delete_file_RH,create_pandas_dataframe_and_delete_file_COSC



cookies_text = '_ga=GA1.1.1628983433.1728449507; 131190129518317761110284151614=s%3AkAHWxuAr73Rh7mAq9nbL8-oPt_Z74XfQ.SQSgjHtZXslo3oq7PAI9f4248t9pin31RuN5gWXFGmI; _ga_WXLRFJLP5B=GS1.1.1740976397.18.0.1740977678.0.0.0; _ga_T0WYSFJPBT=GS1.1.1740978634.3.0.1740978634.0.0.0; _ga_M3PE7MJ3XR=GS1.1.1740978634.1.0.1740978634.0.0.0; _ga_CSLL4ZEK4L=GS1.1.1740978635.19.0.1740978688.0.0.0; urs_user_already_logged=yes; urs_guid_ops=4b121d99-271d-4a52-801f-9e82865336c0; asf-urs=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1cnMtdXNlci1pZCI6InNhbWFubWFkYW5pNzEiLCJmaXJzdF9uYW1lIjoic2FtYW4iLCJsYXN0X25hbWUiOiJtYWRhbmkiLCJlbWFpbCI6InNhbWFuLm1hZGFuaTcxQGdtYWlsLmNvbSIsInVycy1hY2Nlc3MtdG9rZW4iOiJleUowZVhBaU9pSktWMVFpTENKdmNtbG5hVzRpT2lKRllYSjBhR1JoZEdFZ1RHOW5hVzRpTENKemFXY2lPaUpsWkd4cWQzUndkV0pyWlhsZmIzQnpJaXdpWVd4bklqb2lVbE15TlRZaWZRLmV5SjBlWEJsSWpvaVQwRjFkR2dpTENKamJHbGxiblJmYVdRaU9pSmxNbGRXYXpoUWR6WjNaV1ZNVlV0YVdVOTRkbFJSSWl3aVpYaHdJam94TnpRek5UY3dOamt4TENKcFlYUWlPakUzTkRBNU56ZzJPVEVzSW1semN5STZJbWgwZEhCek9pOHZkWEp6TG1WaGNuUm9aR0YwWVM1dVlYTmhMbWR2ZGlJc0luVnBaQ0k2SW5OaGJXRnViV0ZrWVc1cE56RWlMQ0pwWkdWdWRHbDBlVjl3Y205MmFXUmxjaUk2SW1Wa2JGOXZjSE1pTENKaGMzTjFjbUZ1WTJWZmJHVjJaV3dpT2pNc0ltRmpjaUk2SW1Wa2JDSjkuRFBpQjV1WHZxbHYyeVYyYi1OdUtGTlhMMzlUUzFfQlFuN3I0NjVydC12N2FxbTJ0cU9uZExFM1p3WVFRZ3VKYWJDSGtzUHpIcDRqenliWkpXT2hubUV0ZjFRV2pDNDI3SDN2R2NxTDczWWMtWVRSQ2RPUGd4b0llb1ZGWThVbXlVSGZkUHJjYlh0WTVKRkY0QndkQk5FVHk5Y0xfTGFxclRLd3UzN1NBT0JwRzFYYXV3OEZKbjdlNFJpYk1nM1ZxUkdudnRzb3JWNTVQVEd0bjhWVDZrd1FEYklpZi1obXhacWMtbXI0allLOGVfRjVlTEthUWJEVExucjZyNWZLNkdYNVdjYUtmcC1VTV9rUDdRbExwVTdsQVNBak1RbmNTQm0zbEVMY09QenNFNFpxMFN5VzNKNThNUHdZaWNrZkJvSGhXUmExQVpPX3Y1bGhrX0tNYk1nIiwidXJzLWdyb3VwcyI6W10sImlhdCI6MTc0MDk3ODY5MSwiZXhwIjoxNzQxNTgzNDkxfQ.BRO4Bnk4PEKGiA-Vft284PXpSMFz9V-HTOREZ2raeZYLNYioYrW4SoDCodXwQK8VPvb67QYGWyps8LGPGCYicmOnsY2RO7yzXyvNiCDtLyiV_f3aNxd68zPeI4aD_S4lJRmAPW1Iiqia7Qt8h7kn7XO6tmEzeq1hA63GWL4Oxx-Glxk_atJkObyt7b0YDhwxfvUeHWjz3BrCqFTaQMSQH7FgQqaRAMAtuH9DDP6vVQj3AC9ZdW94efvbvSafd4CBtgJj-UR9EJlXL_iw7mjvszIy4Zl9hLU7GJWR6CA2xsGRHgdneJ91cZPyJbpD-soI6HKfrBKsf5BKXtJlmBxS9MD2hr4b2qukjY0tZLNrvXu90a88YQeN7oNrVt9tKwPzsYu2OGZ3ZL1a4P-xJxdcfUTSDF7_hNP188uSunVQwIYwMJsQ5-pLEmu-UizD-nnm2-q8ULyJY47UR5Qeym5CKF1IuTCaEZIy0WsjcrocqtCaGW0DGIyMqPUUgKseIriFxOHCeNIzfiC8uK1DylycqJzQH5zWDUZ33f-HUk3Uniq-CFOpPgKTjc7x3YtxNn64bUYtd_RRmxc8--wvOSC0wk9m0ObOLC7zlr_YyhOcQiVfyObo4NNIvMI4qL-SltooOavFkCfvPpFcT6_cTt7LNXSv4yhsVH1Oss1lKreogGc'

cookies = create_cookies(cookies_text)

async def download_file(session,url,semaphore,log_file):
    async with semaphore:
        try:
            filename = os.path.basename(url)
            async with session.get(url) as response:
                if response.status == 200:
                    with open('NASA_downloads/'+filename,'ab') as file:
                        while True:
                            chunk = await response.content.read(1024)
                            if not chunk:
                                break
                            file.write(chunk)
                    print(f'Downloaded {filename}')
                    if 'statD_2d_slv_Nx' in filename:
                        create_pandas_dataframe_and_delete_file_T2('NASA_downloads/'+filename,filename.split('.')[2])
                    elif 'inst6_3d_ana_Nv' in filename:
                        create_pandas_dataframe_and_delete_file_O3('NASA_downloads/'+filename,filename.split('.')[2])
                    elif 'FLDAS_NOAH01_C_GL_MA' in filename:
                        create_pandas_dataframe_and_delete_file_Rainfall('NASA_downloads/'+filename,filename.split('.')[1][4:])
                    elif 'GLDAS_NOAH025_M' in filename:
                        create_pandas_dataframe_and_delete_file_Rainfall2('NASA_downloads/'+filename, filename.split('.')[1][1:])
                    elif 'tavg1_2d_aer_Nx' in filename:
                        create_pandas_dataframe_and_delete_file_PM25('NASA_downloads/'+filename,filename.split('.')[2])
                    elif 'HAQAST_CNN_L4_V1' in filename:
                        create_pandas_dataframe_and_delete_file_PM25_second_way('NASA_downloads/'+filename,filename.split('.')[1])
                    elif 'inst1_2d_asm_Nx' in filename:
                        create_pandas_dataframe_and_delete_file_TO3('NASA_downloads/'+filename,filename.split('.')[2])
                    elif 'tavg3_3d_cld_Np' in filename:
                        create_pandas_dataframe_and_delete_file_RH('NASA_downloads/'+filename,filename.split('.')[2])
                    elif 'tavg1_2d_chm_Nx' in filename:
                        create_pandas_dataframe_and_delete_file_COSC('NASA_downloads/'+filename,filename.split('.')[2])
                else:
                    raise aiohttp.HttpProcessingError(code=response.status, message="Non-200 status code")        
        except Exception as e:
            print(f"Failed to download {url}: {str(e)}")
            # Log the undownloaded URL with exception details
            async with aiofiles.open(log_file, 'a') as log:
                await log.write(f"{url}\n")
            


async def main():

    #  This array should be replaced
    for path in ['NASA/subset_MERRA2_CNN_HAQAST_PM25_1_20241009_082513_.txt']:

        semaphore = asyncio.Semaphore(1)
        log_file = "undownloaded_urls.log"
        with open(path,'r') as file:
            url_str = file.read()
        urls = url_str.split('\n')
        async with aiohttp.ClientSession(cookies=cookies) as session:
            tasks = [download_file(session, url, semaphore,log_file) for url in urls]
            await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())
