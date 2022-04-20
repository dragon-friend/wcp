#include <sys/stat.h>
#include <sys/resource.h>
#include <sys/utsname.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <filesystem>
#include "Assert.hpp"
#include "CopyQueue.hpp"
#include "Config.hpp"
#include "Util.hpp"
#include <iostream>
#include <filesystem>
#include <cstring>

using namespace std;

int input_errors(int argc, char **argv)
{
	utsname sysinfo;
	uname(&sysinfo);

	if (strncmp("Linux", sysinfo.sysname, 5))
	{
		cerr << "How did you even compile this on" << sysinfo.sysname << "?" << endl;
		return 1;
	}

	int major_version = stoi(sysinfo.release);

	if (major_version < 5 || (major_version == 5 && stoi(strchr(sysinfo.release, '.') + 1) < 6))
	{
		cerr << "Sorry, wcp requires at least Linux 5.6" << endl;
		return 1;
	}

	if(argc != 3)
	{
		cerr << "Usage: " << argv[0] << " SRC DEST" << endl <<
				"Supports copying files and folders." << endl <<
				"Contributions welcome to add better argument handling, and multiple sources!" << endl;
		return 1;
	}

	return 0;
}

size_t getPhysicalRamSize()
{
	long pages = sysconf(_SC_PHYS_PAGES);
	long pageSize = sysconf(_SC_PAGE_SIZE);
	release_assert(pages != -1 && pageSize != -1);
	return size_t(pages) * size_t(pageSize);
}

int wcpMain(int argc, char** argv)
{

	if(input_errors(argc, argv))
		return 1;

    std::filesystem::path src = argv[1];
    std::filesystem::path dest = argv[2];
    release_assert(!src.empty() && !dest.empty());

    size_t oneGig = 1024 * 1024 * 1024;
    size_t ramQuota = std::max(getPhysicalRamSize() / 10, oneGig);
    size_t blockSize = 128 * 1024 * 1024; // 128M
    size_t ringSize = ramQuota / blockSize;
    size_t fileDescriptorCap = 900;
    CopyQueue copyQueue(ringSize, fileDescriptorCap, Heap(ramQuota / blockSize, blockSize));
    copyQueue.start();

    struct statx srcStat = {};
    {
        Result result = myStatx(AT_FDCWD, src, 0, STATX_BASIC_STATS, srcStat);
        if (std::holds_alternative<Error>(result))
        {
            fprintf(stderr, "%s\n", std::get<Error>(result).humanFriendlyErrorMessage->c_str());
            return 1;
        }
    }

    struct statx destStat = {};
    int destStatResult = 0;
    {
        destStatResult = retrySyscall([&]()
        {
            statx(AT_FDCWD, dest.c_str(), 0, STATX_BASIC_STATS, &destStat);
        });

        if (destStatResult != 0 && destStatResult != ENOENT)
        {
            fprintf(stderr, "Failed to stat \"%s\": \"%s\"\n", dest.c_str(), strerror(destStatResult));
            return 1;
        }
    }

    {
        std::filesystem::path destRealParent = std::filesystem::absolute(dest);
        if (!destRealParent.has_filename()) // This is needed to account for if dest has a trailing slash
            destRealParent = destRealParent.parent_path();
        destRealParent = destRealParent.parent_path();

        release_assert(access(destRealParent.c_str(), F_OK) == 0);
    }

    if (S_ISDIR(srcStat.stx_mode))
    {
        dest /= src.filename();
        recursiveMkdir(dest.string());

        if (destStatResult == 0)
            release_assert(S_ISDIR(destStat.stx_mode));

        copyQueue.addRecursiveCopy(src, dest);
    }
    else if (S_ISREG(srcStat.stx_mode))
    {
        if (destStatResult == 0 && S_ISDIR(destStat.stx_mode))
            dest /= src.filename();

        copyQueue.addFileCopy(src, dest, &srcStat);
    }
    else
    {
        release_assert(false);
    }

    CopyQueue::OnCompletionAction completionAction = Config::NO_CLEANUP ? CopyQueue::OnCompletionAction::ExitProcessNoCleanup :
                                                     CopyQueue::OnCompletionAction::Return;

    bool success = copyQueue.join(completionAction);

    return int(!success);
}
