import os   
import zipfile
from flask import Blueprint, render_template, request, jsonify
from config import FB_FOLDER, OHLC_FOLDER, INDEX_OHLC_FOLDER, INDEX_FILES_FOLDER

# Create Blueprint
uploadfile = Blueprint('uploadfile', __name__, template_folder="../templates")

@uploadfile.route('/fileupload')
def fileupload():
    return render_template('fileupload.html')

@uploadfile.route('/uploadfile', methods=['POST'])
def upload_file():
    saved_files = []

    # Handle FB file upload
    for i in range(1, 4):
        fileupload_name = 'FB0'+str(i)+'zip'  
        fb_file = request.files.get(fileupload_name)
        if fb_file and fb_file.filename:
            if fb_file.filename.endswith('.zip'):
                zip_folder_name = os.path.join(FB_FOLDER, os.path.splitext(fb_file.filename)[0])
                os.makedirs(zip_folder_name, exist_ok=True)
                with zipfile.ZipFile(fb_file, 'r') as zip_ref:
                    zip_ref.extractall(zip_folder_name)
                saved_files.append(fb_file.filename)

    # Handle NSE file upload
    nse_file = request.files.get('nse_file')
    if nse_file and nse_file.filename:
        nse_file_path = os.path.join(OHLC_FOLDER, nse_file.filename)
        nse_file.save(nse_file_path)
        saved_files.append(nse_file.filename)

    # Handle BSE file upload
    bse_file = request.files.get('bse_file')
    if bse_file and bse_file.filename:
        bse_file_path = os.path.join(OHLC_FOLDER, bse_file.filename)
        bse_file.save(bse_file_path)
        saved_files.append(bse_file.filename)
        
    # Handle IndexOHLC file upload
    index_ohlc_file = request.files.get('IndexOHLCFile')
    if index_ohlc_file and index_ohlc_file.filename:
        index_ohlc_file_path = os.path.join(INDEX_OHLC_FOLDER, index_ohlc_file.filename)
        index_ohlc_file.save(index_ohlc_file_path)
        saved_files.append(index_ohlc_file.filename)
    
    # Handle index-file1 file upload
    index_file1 = request.files.get('ind_nifty500list.csv')
    if index_file1 and index_file1.filename:
        index_file1_path = os.path.join(INDEX_FILES_FOLDER, index_file1.filename)
        # before saving the file, check filename is ind_nifty500list.csv
        if index_file1.filename == 'ind_nifty500list.csv':
            index_file1.save(index_file1_path)
            saved_files.append(index_file1.filename)
        else:
            return jsonify({'message': 'Invalid file uploaded. Please upload ind_nifty500list.csv file.'})

        
    # Handle index-file2 file upload
    index_file2 = request.files.get('BSE500_Index.csv')
    if index_file2 and index_file2.filename:
        index_file2_path = os.path.join(INDEX_FILES_FOLDER, index_file2.filename)
        if index_file2.filename == 'BSE500_Index.csv':
            index_file2.save(index_file2_path)
            saved_files.append(index_file2.filename)
        else:
            return jsonify({'message': 'Invalid file uploaded. Please upload BSE500_Index.csv file.'})

    # Handle missing files
    if not saved_files:
        return jsonify({'message': 'No files uploaded', 'files': []})

    return jsonify({'message': 'Files uploaded successfully', 'files': saved_files})