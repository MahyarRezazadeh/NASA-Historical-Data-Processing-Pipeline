import aiohttp
import glob
import asyncio
import os
import logging

import aiofiles

from main import create_cookies ,create_pandas_dataframe_and_delete_file_T2,create_pandas_dataframe_and_delete_file_O3,create_pandas_dataframe_and_delete_file_Rainfall,create_pandas_dataframe_and_delete_file_Rainfall2,create_pandas_dataframe_and_delete_file_PM25,create_pandas_dataframe_and_delete_file_PM25_second_way,create_pandas_dataframe_and_delete_file_TO3,create_pandas_dataframe_and_delete_file_RH,create_pandas_dataframe_and_delete_file_COSC



# Earthdata login cookies are personal credentials: set them in the EARTHDATA_COOKIES environment variable.



cookies_text = os.environ.get('EARTHDATA_COOKIES', '')
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
