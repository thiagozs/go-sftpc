package sftpc

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type SFTPClient struct {
	params     *SFTPClientParams
	sshClient  *ssh.Client
	sftpClient *sftp.Client
}

func NewSFTPClient(opts ...Options) (*SFTPClient, error) {
	var authMethods []ssh.AuthMethod
	var signer ssh.Signer

	params, err := newsSFTPClientParams(opts...)
	if err != nil {
		return nil, err
	}

	if params.Password() != "" {
		authMethods = append(authMethods, ssh.Password(params.Password()))
	}

	if params.PrivateKeyPath() != "" {
		key, err := os.ReadFile(params.PrivateKeyPath())
		if err != nil {
			return nil, fmt.Errorf("failed to read private key: %w", err)
		}

		if params.Password() != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase(key, []byte(params.Password()))
			if err != nil {
				return nil, fmt.Errorf("failed to parse private key with passphrase: %w", err)
			}
		} else {
			signer, err = ssh.ParsePrivateKey(key)
			if err != nil {
				return nil, fmt.Errorf("failed to parse private key: %w", err)
			}

		}

		authMethods = append(authMethods, ssh.PublicKeys(signer))

	} else if len(params.PrivateKeyB64()) > 0 {
		if params.Password() != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase(params.PrivateKeyB64(), []byte(params.Password()))
			if err != nil {
				return nil, fmt.Errorf("failed to parse private key with passphrase: %w", err)
			}
		} else {
			signer, err = ssh.ParsePrivateKey(params.PrivateKeyB64())
			if err != nil {
				return nil, fmt.Errorf("failed to parse private key: %w", err)
			}
		}

		authMethods = append(authMethods, ssh.PublicKeys(signer))
	}

	sshConfig := &ssh.ClientConfig{
		User:            params.User(),
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         120 * time.Second,
	}

	addr := fmt.Sprintf("%s:%s", params.Host(), params.Port())
	sshClient, err := ssh.Dial("tcp", addr, sshConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}

	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		sshClient.Close()
		return nil, fmt.Errorf("failed to create SFTP client: %w", err)
	}

	return &SFTPClient{
		params:     params,
		sshClient:  sshClient,
		sftpClient: sftpClient,
	}, nil
}

func (client *SFTPClient) Close() {
	if client.sftpClient != nil {
		client.sftpClient.Close()
	}
	if client.sshClient != nil {
		client.sshClient.Close()
	}
}

func (client *SFTPClient) UploadFile(localPath, remotePath string) error {
	if client == nil {
		return fmt.Errorf("SFTPClient is nil")
	}

	err := client.ensureConnected()
	if err != nil {
		return fmt.Errorf("failed to reconnect: %w", err)
	}

	_, err = os.Stat(localPath)
	if err != nil {
		return fmt.Errorf("failed to get local file info: %w", err)
	}

	remoteFileInfo, err := client.sftpClient.Stat(remotePath)
	var remoteFileSize int64
	if err == nil {
		remoteFileSize = remoteFileInfo.Size()
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to get remote file info: %w", err)
	}

	srcFile, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open local file: %w", err)
	}
	defer srcFile.Close()

	_, err = srcFile.Seek(remoteFileSize, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek in local file: %w", err)
	}

	var dstFile *sftp.File
	//if remoteFileSize > 0 {
	dstFile, err = client.sftpClient.OpenFile(remotePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	//} else {
	//	dstFile, err = client.sftpClient.Create(remotePath)
	//}
	if err != nil {
		return fmt.Errorf("failed to open or create remote file: %w", err)
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		return fmt.Errorf("failed to copy file to remote: %w", err)
	}

	return nil
}

func (client *SFTPClient) DownloadFile(remotePath, localPath string) error {
	if client == nil {
		return fmt.Errorf("SFTPClient is nil")
	}

	// Ensure connection before download
	err := client.ensureConnectedWithRetries(3)
	if err != nil {
		return fmt.Errorf("failed to reconnect: %w", err)
	}

	// Get remote file info
	remoteFileInfo, err := client.sftpClient.Stat(remotePath)
	if err != nil {
		// Skip permission denied errors
		if os.IsPermission(err) {
			log.Printf("Permission denied for file: %s", remotePath)
			return nil // Skip this file
		}
		return fmt.Errorf("failed to get remote file info: %w", err)
	}
	remoteFileSize := remoteFileInfo.Size()

	// Get local file info to resume download
	var localFileSize int64
	localFileInfo, err := os.Stat(localPath)
	if err == nil {
		localFileSize = localFileInfo.Size()
		if localFileSize == remoteFileSize {
			log.Printf("File already fully downloaded: %s", localPath)
			return nil // File is fully downloaded
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to get local file info: %w", err)
	}

	// Open the remote file
	remoteFile, err := client.sftpClient.Open(remotePath)
	if err != nil {
		return fmt.Errorf("failed to open remote file: %w", err)
	}
	defer remoteFile.Close()

	// Seek in the remote file to resume download
	_, err = remoteFile.Seek(localFileSize, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek in remote file: %w", err)
	}

	// Open the local file for append or create if it doesn't exist
	var localFile *os.File
	if localFileSize > 0 {
		localFile, err = os.OpenFile(localPath, os.O_WRONLY|os.O_APPEND, 0644)
	} else {
		localFile, err = os.Create(localPath)
	}
	if err != nil {
		return fmt.Errorf("failed to open or create local file: %w", err)
	}
	defer localFile.Close()

	// Retry download loop
	for retries := 0; retries < 3; retries++ {
		_, err = io.Copy(localFile, remoteFile)
		if err != nil {
			if retries < 2 {
				log.Printf("Download failed, retrying... attempt %d", retries+1)
				time.Sleep(5 * time.Second)
				err = client.ensureConnectedWithRetries(3) // Ensure reconnection before retry
				if err != nil {
					return fmt.Errorf("failed to reconnect: %w", err)
				}
			} else {
				return fmt.Errorf("failed to copy file to local after 3 retries: %w", err)
			}
		} else {
			break // Download successful, exit retry loop
		}
	}

	log.Printf("Resumed and downloaded file: %s", localPath)
	return nil
}

func (client *SFTPClient) RemoveFile(remotePath string) error {
	if client == nil {
		return fmt.Errorf("SFTPClient is nil")
	}
	err := client.sftpClient.Remove(remotePath)
	if err != nil {
		return fmt.Errorf("failed to remove remote file: %w", err)
	}
	//log.Println("File removed successfully")
	return nil
}

func (client *SFTPClient) MoveFile(oldPath, newPath string) error {
	if client == nil {
		return fmt.Errorf("SFTPClient is nil")
	}
	err := client.sftpClient.Rename(oldPath, newPath)
	if err != nil {
		return fmt.Errorf("failed to move remote file: %w", err)
	}
	return nil
}

func (client *SFTPClient) List(remotePath string) ([]os.FileInfo, error) {
	if client == nil {
		return nil, fmt.Errorf("SFTPClient is nil")
	}
	files, err := client.sftpClient.ReadDir(remotePath)
	if err != nil {
		return nil, fmt.Errorf("failed to list directory: %w", err)
	}
	return files, nil
}

func (client *SFTPClient) MakeDir(remotePath string) error {
	if client == nil {
		return fmt.Errorf("SFTPClient is nil")
	}
	err := client.sftpClient.Mkdir(remotePath)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	return nil
}

func (client *SFTPClient) RemoveDir(remotePath string) error {
	if client == nil {
		return fmt.Errorf("SFTPClient is nil")
	}
	err := client.sftpClient.RemoveDirectory(remotePath)
	if err != nil {
		return fmt.Errorf("failed to remove directory: %w", err)
	}
	return nil
}

func (client *SFTPClient) MoveDir(oldPath, newPath string) error {
	if client == nil {
		return fmt.Errorf("SFTPClient is nil")
	}
	err := client.sftpClient.Rename(oldPath, newPath)
	if err != nil {
		return fmt.Errorf("failed to move directory: %w", err)
	}
	return nil
}

func (client *SFTPClient) ListDirs(remotePath string) ([]os.FileInfo, error) {
	if client == nil {
		return nil, fmt.Errorf("SFTPClient is nil")
	}
	dirs, err := client.sftpClient.ReadDir(remotePath)
	if err != nil {
		return nil, fmt.Errorf("failed to list directory: %w", err)
	}

	var result []os.FileInfo
	for _, dir := range dirs {
		if dir.IsDir() {
			result = append(result, dir)
		}
	}
	return result, nil
}

func (client *SFTPClient) ListFiles(remotePath string) ([]os.FileInfo, error) {
	if client == nil {
		return nil, fmt.Errorf("SFTPClient is nil")
	}
	files, err := client.sftpClient.ReadDir(remotePath)
	if err != nil {
		return nil, fmt.Errorf("failed to list directory: %w", err)
	}

	var result []os.FileInfo
	for _, file := range files {
		if !file.IsDir() {
			result = append(result, file)
		}
	}
	return result, nil
}

func (client *SFTPClient) ReConnect() error {
	var authMethods []ssh.AuthMethod
	var signerIn ssh.Signer

	// Close previous connections if they exist
	if client.sftpClient != nil {
		client.sftpClient.Close()
	}
	if client.sshClient != nil {
		client.sshClient.Close()
	}

	if client.params.Password() != "" {
		authMethods = append(authMethods, ssh.Password(client.params.Password()))
	}

	if client.params.PrivateKeyPath() != "" {
		key, err := os.ReadFile(client.params.PrivateKeyPath())
		if err != nil {
			return fmt.Errorf("failed to read private key: %w", err)
		}

		if client.params.Password() != "" {
			signer, err := ssh.ParsePrivateKeyWithPassphrase(key, []byte(client.params.Password()))
			if err != nil {
				return fmt.Errorf("failed to parse private key with passphrase: %w", err)
			}
			signerIn = signer
		} else {
			signer, err := ssh.ParsePrivateKey(key)
			if err != nil {
				return fmt.Errorf("failed to parse private key: %w", err)
			}
			signerIn = signer

		}

		authMethods = append(authMethods, ssh.PublicKeys(signerIn))

	} else if len(client.params.PrivateKeyB64()) > 0 {
		if client.params.Password() != "" {
			signer, err := ssh.ParsePrivateKeyWithPassphrase(client.params.PrivateKeyB64(), []byte(client.params.Password()))
			if err != nil {
				return fmt.Errorf("failed to parse private key with passphrase: %w", err)
			}
			signerIn = signer
		} else {
			signer, err := ssh.ParsePrivateKey(client.params.PrivateKeyB64())
			if err != nil {
				return fmt.Errorf("failed to parse private key: %w", err)
			}
			signerIn = signer
		}

		authMethods = append(authMethods, ssh.PublicKeys(signerIn))
	}

	sshConfig := &ssh.ClientConfig{
		User:            client.params.User(),
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         180 * time.Second,
	}

	addr := fmt.Sprintf("%s:%s", client.params.Host(), client.params.Port())
	sshClient, err := ssh.Dial("tcp", addr, sshConfig)
	if err != nil {
		return fmt.Errorf("failed to dial: %w", err)
	}

	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		sshClient.Close()
		return fmt.Errorf("failed to create SFTP client: %w", err)
	}

	client.sshClient = sshClient
	client.sftpClient = sftpClient

	return nil
}

func (client *SFTPClient) FolderExists(remotePath string) bool {
	if client == nil {
		return false
	}
	_, err := client.sftpClient.Stat(remotePath)

	return err == nil
}

func (client *SFTPClient) FileExists(remotePath string) bool {
	if client == nil {
		return false
	}
	_, err := client.sftpClient.Stat(remotePath)
	if err != nil {
		return false
	}
	return true
}

func (client *SFTPClient) ListFilesAndFolders(remotePath string) ([]os.FileInfo, error) {
	if client == nil {
		return nil, fmt.Errorf("SFTPClient is nil")
	}
	files, err := client.sftpClient.ReadDir(remotePath)
	if err != nil {
		return nil, fmt.Errorf("failed to list directory: %w", err)
	}
	return files, nil
}

func (client *SFTPClient) WalkFile(remotePath string, walkFn func(path string, info os.FileInfo) error) error {
	if client == nil {
		return fmt.Errorf("SFTPClient is nil")
	}

	// Normalize the path by stripping leading slash if needed
	normalizedPath := remotePath
	if len(remotePath) > 1 && remotePath[0] == '/' {
		normalizedPath = remotePath[1:]
	}

	files, err := client.sftpClient.ReadDir(normalizedPath)
	if err != nil {
		// Handle permission denied error
		if os.IsPermission(err) {
			log.Printf("permission denied: %s", normalizedPath)
			return nil // Skip this directory and continue
		}

		// Handle file does not exist error
		if os.IsNotExist(err) {
			log.Printf("file or directory does not exist: %s", normalizedPath)
			return nil // Skip and continue
		}

		// Retry without the leading slash if path exists but failed
		if normalizedPath != remotePath {
			log.Printf("retrying without leading slash: %s", normalizedPath)
			files, err = client.sftpClient.ReadDir(normalizedPath)
			if err != nil {
				return fmt.Errorf("failed to list directory after retry: %w", err) // Stop recursion
			}
		} else {
			return fmt.Errorf("failed to list directory: %w", err) // Stop recursion
		}
	}

	for _, file := range files {
		fullPath := normalizedPath + "/" + file.Name()
		err = walkFn(fullPath, file)
		if err != nil {
			return err
		}

		// If the file is a directory, recursively walk into it
		if file.IsDir() {
			err = client.WalkFile(fullPath, walkFn)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (client *SFTPClient) ensureConnectedWithRetries(retries int) error {
	for i := 0; i < retries; i++ {
		err := client.ensureConnected()
		if err == nil {
			return nil
		}
		log.Printf("Reconnection attempt %d failed: %v", i+1, err)
		time.Sleep(2 * time.Second) // Sleep before retrying
	}
	return fmt.Errorf("failed to reconnect after %d attempts", retries)
}

func (client *SFTPClient) ensureConnected() error {
	if client.isConnected() {
		return nil // Connection is fine
	}
	// Try reconnecting
	return client.ReConnect()
}

func (client *SFTPClient) isConnected() bool {
	if client == nil || client.sftpClient == nil || client.sshClient == nil {
		return false
	}
	// Try a simple operation to check if the connection is active
	_, err := client.sftpClient.ReadDir(".")
	return err == nil
}

// CreateRemoteDirRecursive creates remote directories recursively starting from the first missing directory.
// It ensures the correct relative path is built for the remoteBasePath.
func (client *SFTPClient) CreateRemoteDirRecursive(remoteBasePath string) error {
	// // Ensure that the local path contains the relevant folder part after the base path
	// baseIndex := strings.LastIndex(fullLocalPath, remoteBasePath)
	// if baseIndex == -1 {
	// 	// Instead of returning an error, return nil and skip processing if remoteBasePath is not found
	// 	log.Printf("remoteBasePath '%s' not found in local path '%s', skipping...\n", remoteBasePath, fullLocalPath)
	// 	return nil
	// }

	// // Get the relative directory path starting from remoteBasePath
	// relativeDir := fullLocalPath[baseIndex+len(remoteBasePath):]

	// // Combine remoteBasePath and the relative path to form the full remote directory structure
	// relativeDir = filepath.Join(remoteBasePath, relativeDir)

	// Split the relative path into directories
	dirs := strings.Split(remoteBasePath, string(filepath.Separator))
	var currentPath string

	// Iterate through the directories and create each if missing
	for _, dir := range dirs {
		if dir == "" {
			continue // Skip any empty components
		}

		// Build the current path
		if currentPath == "" {
			currentPath = dir
		} else {
			currentPath = filepath.Join(currentPath, dir)
		}

		// Check if the current directory exists
		if !client.FolderExists(currentPath) {
			// Create the directory if it doesn't exist
			err := client.MakeDir(currentPath)
			if err != nil {
				return fmt.Errorf("failed to create directory '%s', error: %v", currentPath, err)
			}
			log.Printf("Created remote directory: %s\n", currentPath)
		} else {
			log.Printf("Directory already exists: %s\n", currentPath)
		}
	}
	return nil
}

func (client *SFTPClient) UploadFileWithProgress(localPath, remotePath string) error {
	if client == nil {
		return fmt.Errorf("SFTPClient is nil")
	}

	// Ensure connection
	err := client.ensureConnected()
	if err != nil {
		return fmt.Errorf("failed to reconnect: %w", err)
	}

	// Get local file info
	localFileInfo, err := os.Stat(localPath)
	if err != nil {
		return fmt.Errorf("failed to get local file info: %w", err)
	}
	localFileSize := localFileInfo.Size()

	// Open the local file
	localFile, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open local file: %w", err)
	}
	defer localFile.Close()

	// Check if the remote file already exists and get its size
	remoteFileInfo, err := client.sftpClient.Stat(remotePath)
	var remoteFileSize int64
	if err == nil {
		remoteFileSize = remoteFileInfo.Size()
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to get remote file info: %w", err)
	}

	// Seek in the local file to resume upload from where it left off
	_, err = localFile.Seek(remoteFileSize, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek in local file: %w", err)
	}

	// Open or create the remote file
	//remoteFile, err := client.sftpClient.OpenFile(remotePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)

	remoteFile, err := client.sftpClient.Create(remotePath)
	if err != nil {
		return fmt.Errorf("failed to open or create remote file: %w", err)
	}
	defer remoteFile.Close()

	// Upload file with progress tracking
	buffer := make([]byte, 32*1024) // 32 KB buffer
	var totalBytesRead int64

	for {
		n, readErr := localFile.Read(buffer)
		if n > 0 {
			_, writeErr := remoteFile.Write(buffer[:n])
			if writeErr != nil {
				return fmt.Errorf("failed to write to remote file: %w", writeErr)
			}

			totalBytesRead += int64(n)
			percent := float64(totalBytesRead) / float64(localFileSize) * 100
			fmt.Printf("\rUploading... %.2f%% complete", percent)
		}

		if readErr != nil {
			if readErr == io.EOF {
				break // End of file reached
			}
			return fmt.Errorf("failed to read from local file: %w", readErr)
		}
	}

	fmt.Println("\nFile uploaded successfully")
	return nil
}

func (client *SFTPClient) DownloadFileWithProgress(remotePath, localPath string) error {
	if client == nil {
		return fmt.Errorf("SFTPClient is nil")
	}

	// Ensure connection before download
	err := client.ensureConnectedWithRetries(3)
	if err != nil {
		return fmt.Errorf("failed to reconnect: %w", err)
	}

	// Get remote file info
	remoteFileInfo, err := client.sftpClient.Stat(remotePath)
	if err != nil {
		// Skip permission denied errors
		if os.IsPermission(err) {
			log.Printf("Permission denied for file: %s", remotePath)
			return nil // Skip this file
		}
		return fmt.Errorf("failed to get remote file info: %w", err)
	}
	remoteFileSize := remoteFileInfo.Size()

	// Get local file info to resume download
	var localFileSize int64
	localFileInfo, err := os.Stat(localPath)
	if err == nil {
		localFileSize = localFileInfo.Size()
		if localFileSize == remoteFileSize {
			log.Printf("File already fully downloaded: %s", localPath)
			return nil // File is fully downloaded
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to get local file info: %w", err)
	}

	// Open the remote file
	remoteFile, err := client.sftpClient.Open(remotePath)
	if err != nil {
		return fmt.Errorf("failed to open remote file: %w", err)
	}
	defer remoteFile.Close()

	// Seek in the remote file to resume download
	_, err = remoteFile.Seek(localFileSize, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek in remote file: %w", err)
	}

	// Open the local file for append or create if it doesn't exist
	var localFile *os.File
	if localFileSize > 0 {
		localFile, err = os.OpenFile(localPath, os.O_WRONLY|os.O_APPEND, 0644)
	} else {
		localFile, err = os.Create(localPath)
	}
	if err != nil {
		return fmt.Errorf("failed to open or create local file: %w", err)
	}
	defer localFile.Close()

	// Download the file with progress tracking
	buffer := make([]byte, 32*1024) // 32 KB buffer
	var totalBytesRead int64 = localFileSize

	for {
		n, readErr := remoteFile.Read(buffer)
		if n > 0 {
			_, writeErr := localFile.Write(buffer[:n])
			if writeErr != nil {
				return fmt.Errorf("failed to write to local file: %w", writeErr)
			}

			totalBytesRead += int64(n)
			percent := float64(totalBytesRead) / float64(remoteFileSize) * 100
			fmt.Printf("\rDownloading... %.2f%% complete", percent)
		}

		if readErr != nil {
			if readErr == io.EOF {
				break // End of file reached
			}
			return fmt.Errorf("failed to read from remote file: %w", readErr)
		}
	}

	fmt.Println("\nFile downloaded successfully")
	return nil
}

func (client *SFTPClient) FileInfo(filePath string) (os.FileInfo, error) {
	if client == nil {
		return nil, fmt.Errorf("SFTPClient is nil")
	}

	fileInfo, err := client.sftpClient.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	return fileInfo, nil
}
